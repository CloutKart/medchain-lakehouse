/* Heatmap — occupancy across hospital × ward type.
 *
 * A grid of magnitudes, so: one sequential hue, light to dark, never a rainbow. The
 * ramp is a single blue with monotonic lightness, which means the encoding survives
 * greyscale printing and every form of colour vision deficiency — cells are ordered
 * by how dark they are, not by hue.
 *
 * Every cell carries its value as text. That is what discharges the contrast
 * obligation on the lighter steps of the ramp, and it means the chart is readable
 * without relying on the colour at all.
 */

import { useState } from "react";
import { Figure, Table, Tooltip, type TipState } from "./chrome";

const RAMP = [
  "var(--seq-100)",
  "var(--seq-200)",
  "var(--seq-300)",
  "var(--seq-400)",
  "var(--seq-500)",
  "var(--seq-600)",
  "var(--seq-700)",
];

export interface HeatCell {
  row: string;
  col: string;
  value: number;
  extra?: [string, string][];
}

export function Heatmap({
  cells,
  title,
  subtitle,
  note,
  format,
  colOrder,
}: {
  cells: HeatCell[];
  title: string;
  subtitle?: string;
  note?: string;
  format: (v: number) => string;
  colOrder?: string[];
}) {
  const [tip, setTip] = useState<TipState | null>(null);

  const rows = [...new Set(cells.map((c) => c.row))].sort();
  const cols = colOrder
    ? colOrder.filter((c) => cells.some((x) => x.col === c))
    : [...new Set(cells.map((c) => c.col))].sort();

  const values = cells.map((c) => c.value);
  const lo = Math.min(...values);
  const hi = Math.max(...values);

  const step = (v: number) => {
    const t = hi === lo ? 0.5 : (v - lo) / (hi - lo);
    return RAMP[Math.min(RAMP.length - 1, Math.floor(t * RAMP.length))];
  };
  // Ink flips to light once the cell is dark enough to need it.
  const ink = (v: number) => {
    const t = hi === lo ? 0.5 : (v - lo) / (hi - lo);
    return t > 0.55 ? "#ffffff" : "var(--ink)";
  };

  const cellW = 92;
  const cellH = 40;
  const labelW = 196;
  const headH = 34;
  const width = labelW + cols.length * cellW;
  const height = headH + rows.length * cellH;

  const lookup = new Map(cells.map((c) => [`${c.row}|${c.col}`, c]));

  return (
    <Figure
      title={title}
      subtitle={subtitle}
      note={note}
      table={
        <Table
          columns={["", ...cols]}
          align={["l", ...cols.map(() => "r" as const)]}
          rows={rows.map((r) => [
            r,
            ...cols.map((c) => {
              const cell = lookup.get(`${r}|${c}`);
              return cell ? format(cell.value) : "—";
            }),
          ])}
        />
      }
    >
      <div className="plot plot--scroll" onMouseLeave={() => setTip(null)}>
        <svg viewBox={`0 0 ${width} ${height}`} width={width} role="img" aria-label={title}>
          {cols.map((c, ci) => (
            <text
              key={c}
              x={labelW + ci * cellW + cellW / 2}
              y={headH - 12}
              textAnchor="middle"
              fontSize={11}
              fill="var(--ink-muted)"
            >
              {c.replace("_", " ")}
            </text>
          ))}

          {rows.map((r, ri) => (
            <g key={r}>
              <text
                x={labelW - 12}
                y={headH + ri * cellH + cellH / 2}
                dy="0.32em"
                textAnchor="end"
                fontSize={12}
                fill="var(--ink-secondary)"
              >
                {r.replace("MedChain ", "")}
              </text>
              {cols.map((c, ci) => {
                const cell = lookup.get(`${r}|${c}`);
                if (!cell) {
                  return (
                    <rect
                      key={c}
                      x={labelW + ci * cellW + 1}
                      y={headH + ri * cellH + 1}
                      width={cellW - 2}
                      height={cellH - 2}
                      fill="var(--plane)"
                    />
                  );
                }
                return (
                  <g
                    key={c}
                    onMouseMove={(e) => {
                      const box = (e.currentTarget.ownerSVGElement as SVGSVGElement)
                        .parentElement!.getBoundingClientRect();
                      setTip({
                        x: e.clientX - box.left,
                        y: e.clientY - box.top,
                        title: `${r.replace("MedChain ", "")} · ${c.replace("_", " ")}`,
                        rows: [["Occupancy", format(cell.value)], ...(cell.extra ?? [])],
                      });
                    }}
                  >
                    {/* 2px surface gap between cells so adjacent fills stay separable
                        without drawing a border on every cell. */}
                    <rect
                      x={labelW + ci * cellW + 1}
                      y={headH + ri * cellH + 1}
                      width={cellW - 2}
                      height={cellH - 2}
                      fill={step(cell.value)}
                      rx={2}
                    />
                    <text
                      x={labelW + ci * cellW + cellW / 2}
                      y={headH + ri * cellH + cellH / 2}
                      dy="0.32em"
                      textAnchor="middle"
                      fontSize={11}
                      fontFamily="var(--font-mono)"
                      fill={ink(cell.value)}
                    >
                      {format(cell.value)}
                    </text>
                  </g>
                );
              })}
            </g>
          ))}
        </svg>
        <Tooltip tip={tip} width={width} />
        <div className="ramp">
          <span className="ramp__label">{format(lo)}</span>
          {RAMP.map((c) => (
            <span key={c} className="ramp__step" style={{ background: c }} />
          ))}
          <span className="ramp__label">{format(hi)}</span>
        </div>
      </div>
    </Figure>
  );
}
