/* Shell and presentation components.
 *
 * The hierarchy here is the fix for "doesn't tell the story": a hero figure states a
 * finding in words and a number, a stat tile carries a supporting measure, and charts
 * come third as the evidence. The old dashboard had only charts, so a reader had to
 * derive the findings themselves — which nobody does.
 */

import type { ReactNode } from "react";

/* ------------------------------------------------------------ hero figure */

export function HeroFigure({
  value,
  unit,
  claim,
  detail,
  href,
  tone = "accent",
}: {
  value: string;
  unit?: string;
  /** The finding, in words. Without this the number is trivia. */
  claim: string;
  detail: string;
  href: string;
  tone?: "accent" | "warn" | "neutral";
}) {
  return (
    <a className={`hero hero--${tone}`} href={href}>
      <div className="hero__figure num">
        {value}
        {unit && <span className="hero__unit">{unit}</span>}
      </div>
      <div className="hero__claim">{claim}</div>
      <p className="hero__detail">{detail}</p>
      <span className="hero__more" aria-hidden="true">
        See the evidence →
      </span>
    </a>
  );
}

/* -------------------------------------------------------------- stat tile */

export function StatTile({
  label,
  value,
  sub,
  emphasis,
}: {
  label: string;
  value: string;
  sub?: string;
  emphasis?: boolean;
}) {
  return (
    <div className={`stat${emphasis ? " stat--emph" : ""}`}>
      <div className="stat__label">{label}</div>
      <div className="stat__value num">{value}</div>
      {sub && <div className="stat__sub">{sub}</div>}
    </div>
  );
}

export function StatRow({ children }: { children: ReactNode }) {
  return <div className="statrow">{children}</div>;
}

/* ---------------------------------------------------------- status pill */

export type Status = "good" | "warning" | "serious" | "critical";

const GLYPH: Record<Status, string> = {
  good: "✓",
  warning: "!",
  serious: "!",
  critical: "✕",
};

/** Status is never carried by colour alone — every pill has a glyph and a word. */
export function StatusPill({ status, children }: { status: Status; children: ReactNode }) {
  return (
    <span className={`pill pill--${status}`}>
      <span className="pill__glyph" aria-hidden="true">
        {GLYPH[status]}
      </span>
      {children}
    </span>
  );
}

/* ----------------------------------------------------------- section */

export function Section({
  id,
  eyebrow,
  title,
  lede,
  children,
}: {
  id: string;
  eyebrow: string;
  title: string;
  lede?: string;
  children: ReactNode;
}) {
  return (
    <section className="section" id={id}>
      <header className="section__head">
        <p className="section__eyebrow">{eyebrow}</p>
        <h2 className="section__title">{title}</h2>
        {lede && <p className="section__lede">{lede}</p>}
      </header>
      {children}
    </section>
  );
}

export function Grid({ children, cols = 2 }: { children: ReactNode; cols?: 1 | 2 }) {
  return <div className={`grid grid--${cols}`}>{children}</div>;
}

/* ------------------------------------------------------------------ rail */

export interface NavItem {
  id: string;
  label: string;
  hint: string;
}

export function Rail({
  items,
  active,
  theme,
  onTheme,
  generatedAt,
  environment,
}: {
  items: NavItem[];
  active: string;
  theme: "light" | "dark";
  onTheme: () => void;
  generatedAt: string;
  environment: string;
}) {
  return (
    <nav className="rail" aria-label="Sections">
      <div className="rail__brand">
        <div className="rail__mark" aria-hidden="true">
          <svg viewBox="0 0 32 32" width="30" height="30">
            {/* Eight arcs for eight hospitals, joined at a centre — the network the
                Master Patient Index reconstructs. */}
            {Array.from({ length: 8 }).map((_, i) => {
              const a = (i / 8) * Math.PI * 2 - Math.PI / 2;
              return (
                <line
                  key={i}
                  x1={16}
                  y1={16}
                  x2={16 + Math.cos(a) * 12}
                  y2={16 + Math.sin(a) * 12}
                  stroke="var(--cat-1)"
                  strokeWidth={1.5}
                  strokeOpacity={0.5}
                  strokeLinecap="round"
                />
              );
            })}
            <circle cx={16} cy={16} r={4} fill="var(--cat-1)" />
          </svg>
        </div>
        <div>
          <div className="rail__name">MedChain</div>
          <div className="rail__sub">Analytics platform</div>
        </div>
      </div>

      <ul className="rail__list">
        {items.map((i) => (
          <li key={i.id}>
            <a
              href={`#${i.id}`}
              className={active === i.id ? "is-active" : undefined}
              // Screen readers get the current section from this, not from the
              // border colour that sights users read it from.
              aria-current={active === i.id ? "true" : undefined}
            >
              <span className="rail__label">{i.label}</span>
              <span className="rail__hint">{i.hint}</span>
            </a>
          </li>
        ))}
      </ul>

      <div className="rail__foot">
        <button type="button" className="rail__theme" onClick={onTheme}>
          {theme === "light" ? "Dark" : "Light"} mode
        </button>
        <dl className="rail__meta">
          <div>
            <dt>Source</dt>
            <dd>Gold layer · {environment}</dd>
          </div>
          <div>
            <dt>Exported</dt>
            <dd>{new Date(generatedAt).toLocaleString("en-GB", { dateStyle: "medium", timeStyle: "short" })}</dd>
          </div>
        </dl>
      </div>
    </nav>
  );
}
