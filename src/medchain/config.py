"""Environment-aware configuration.

The whole portability story of this project rests on this module. Transformation
code never knows whether it is running against a local directory or ADLS Gen2 — it
asks the config for a table path and gets back either ``./data/silver/mpi_registry``
or ``abfss://silver@stmedchain.dfs.core.windows.net/mpi_registry``.

Selection is by the ``MEDCHAIN_ENV`` environment variable (``local`` or ``azure``),
defaulting to ``local`` so nothing accidentally reaches for cloud credentials.
"""

from __future__ import annotations

import os
import re
from dataclasses import dataclass, field
from datetime import date
from pathlib import Path
from typing import Any

import yaml


class ConfigError(RuntimeError):
    """Raised when configuration is missing or internally inconsistent."""


def _resolve_conf_dir() -> Path:
    """Locate the ``conf`` directory, whichever way the package was installed.

    Three cases, in priority order:

    1. ``MEDCHAIN_CONF_DIR`` — an explicit override, for pointing a cluster at
       configuration deployed separately from the wheel.
    2. ``medchain/conf`` inside the installed package. ``pyproject.toml``
       force-includes the repository's ``conf`` tree here, so a wheel is
       self-contained.
    3. ``<repo>/conf`` two levels above this file — the editable-install and
       source-checkout layout.

    Case 2 is why this is a function rather than a one-line expression. Resolving
    only against ``parents[2]`` works perfectly in the repository and silently
    points at ``<site-packages>/../../conf`` once installed as a wheel, which is
    where a Databricks cluster would look for it and not find it.
    """
    override = os.environ.get("MEDCHAIN_CONF_DIR")
    if override:
        candidate = Path(override).expanduser().resolve()
        if not (candidate / "base.yaml").exists():
            raise ConfigError(f"MEDCHAIN_CONF_DIR={override} does not contain base.yaml")
        return candidate

    here = Path(__file__).resolve()
    for candidate in (here.parent / "conf", here.parents[2] / "conf"):
        if (candidate / "base.yaml").exists():
            return candidate

    raise ConfigError(
        "Cannot locate the conf directory. Looked for a packaged copy at "
        f"{here.parent / 'conf'} and a source checkout at {here.parents[2] / 'conf'}. "
        "Set MEDCHAIN_CONF_DIR to point at it explicitly."
    )


CONF_DIR = _resolve_conf_dir()

_VAR_PATTERN = re.compile(r"\$\{([A-Za-z_][A-Za-z0-9_]*)\}")

# Layers that map to a physical storage location.
LAYERS = ("landing", "bronze", "silver", "gold", "control", "quarantine", "checkpoints", "truth")


def _deep_merge(base: dict[str, Any], override: dict[str, Any]) -> dict[str, Any]:
    """Recursively merge ``override`` onto ``base`` without mutating either."""
    merged = dict(base)
    for key, value in override.items():
        if key in merged and isinstance(merged[key], dict) and isinstance(value, dict):
            merged[key] = _deep_merge(merged[key], value)
        else:
            merged[key] = value
    return merged


def _substitute(value: Any, variables: dict[str, str]) -> Any:
    """Expand ``${VAR}`` references from ``variables`` then the process environment.

    An unresolved variable is an error rather than an empty string: silently
    producing ``abfss://silver@.dfs.core.windows.net`` would fail much later and
    much less legibly.
    """
    if isinstance(value, dict):
        return {k: _substitute(v, variables) for k, v in value.items()}
    if isinstance(value, list):
        return [_substitute(v, variables) for v in value]
    if not isinstance(value, str):
        return value

    def replace(match: re.Match[str]) -> str:
        name = match.group(1)
        resolved = variables.get(name, os.environ.get(name))
        if resolved is None:
            raise ConfigError(
                f"Config references ${{{name}}} but it is not set. "
                f"Export it before running (e.g. export {name}=...)."
            )
        return resolved

    return _VAR_PATTERN.sub(replace, value)


@dataclass(frozen=True)
class Config:
    """Resolved configuration for one environment."""

    env: str
    raw: dict[str, Any]
    paths: dict[str, str]
    catalog: str | None
    sources: dict[str, Any]

    # ---- storage addressing -------------------------------------------------

    def path(self, layer: str) -> str:
        """Root URI for a layer, e.g. ``path("silver")``."""
        if layer not in self.paths:
            raise ConfigError(f"Unknown layer {layer!r}; known layers: {sorted(self.paths)}")
        return self.paths[layer]

    def table_path(self, layer: str, table: str) -> str:
        """Physical Delta location for a table.

        Tables are always addressed by path, on every environment. Unity Catalog
        registration (see :meth:`table_fqn`) is layered on top as an external
        table so the catalog and the physical layout never disagree.
        """
        return f"{self.path(layer).rstrip('/')}/{table}"

    def table_fqn(self, layer: str, table: str) -> str | None:
        """Three-level Unity Catalog name, or ``None`` when no catalog is configured."""
        if not self.catalog:
            return None
        return f"{self.catalog}.{layer}.{table}"

    # ---- convenience accessors ---------------------------------------------

    @property
    def is_azure(self) -> bool:
        return self.env == "azure"

    @property
    def window_start(self) -> date:
        return date.fromisoformat(self.raw["window"]["start"])

    @property
    def window_end(self) -> date:
        return date.fromisoformat(self.raw["window"]["end"])

    @property
    def spark_conf(self) -> dict[str, str]:
        """Spark settings from base.yaml merged with the environment's overrides."""
        return {str(k): str(v) for k, v in self.raw.get("spark", {}).get("conf", {}).items()}

    @property
    def spark_master(self) -> str | None:
        return self.raw.get("spark", {}).get("master")

    @property
    def app_name(self) -> str:
        return self.raw.get("spark", {}).get("app_name", "medchain")

    def source(self, name: str) -> dict[str, Any]:
        if name not in self.sources:
            raise ConfigError(f"Unknown source {name!r}; known: {sorted(self.sources)}")
        return self.sources[name]

    @property
    def source_names(self) -> list[str]:
        return sorted(self.sources)

    def get(self, *keys: str, default: Any = None) -> Any:
        """Nested lookup into the raw config, e.g. ``cfg.get("mpi", "weights")``."""
        node: Any = self.raw
        for key in keys:
            if not isinstance(node, dict) or key not in node:
                return default
            node = node[key]
        return node

    @property
    def seed_dir(self) -> Path:
        return CONF_DIR / "seed"


@dataclass
class _Loader:
    conf_dir: Path = CONF_DIR
    _cache: dict[str, Config] = field(default_factory=dict)

    def load(self, env: str | None = None, *, refresh: bool = False) -> Config:
        env = env or os.environ.get("MEDCHAIN_ENV", "local")
        if not refresh and env in self._cache:
            return self._cache[env]

        base_file = self.conf_dir / "base.yaml"
        env_file = self.conf_dir / f"{env}.yaml"
        sources_file = self.conf_dir / "sources.yaml"

        for f in (base_file, env_file, sources_file):
            if not f.exists():
                raise ConfigError(f"Missing config file: {f}")

        base = yaml.safe_load(base_file.read_text()) or {}
        env_cfg = yaml.safe_load(env_file.read_text()) or {}
        merged = _deep_merge(base, env_cfg)

        # `root` is resolved first so path templates can reference it.
        variables: dict[str, str] = {}
        if merged.get("root"):
            variables["root"] = _substitute(merged["root"], variables)
            merged["root"] = variables["root"]

        merged = _substitute(merged, variables)

        paths = merged.get("paths") or {}
        missing = [layer for layer in LAYERS if layer not in paths]
        if missing:
            raise ConfigError(f"{env_file} is missing paths for layers: {missing}")

        # Local paths are made absolute so behaviour does not depend on the
        # working directory a notebook or test happens to start in.
        if env == "local":
            project_root = self.conf_dir.parent
            paths = {
                layer: str((project_root / p).resolve())
                if not p.startswith(("/", "abfss:", "file:"))
                else p
                for layer, p in paths.items()
            }

        sources = (yaml.safe_load(sources_file.read_text()) or {}).get("sources", {})

        cfg = Config(
            env=env,
            raw=merged,
            paths=paths,
            catalog=merged.get("catalog"),
            sources=sources,
        )
        self._cache[env] = cfg
        return cfg


_loader = _Loader()


def load_config(env: str | None = None, *, refresh: bool = False) -> Config:
    """Load (and memoise) the config for ``env``, defaulting to ``$MEDCHAIN_ENV``."""
    return _loader.load(env, refresh=refresh)
