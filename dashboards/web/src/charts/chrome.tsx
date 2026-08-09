/* Shared chart chrome: frame, axes, gridlines, tooltip, table toggle.
 *
 * Every chart in this dashboard is hand-written SVG over d3-scale. That is a
 * deliberate choice, not an oversight: charting libraries impose their own visual
 * defaults — thick marks, boxed legends, heavy grids — and fighting those defaults
 * costs more than drawing the marks. The mark specs this dashboard follows (thin
 * marks, 4px rounded data-ends anchored to the baseline, a 2px surface gap between
 * adjacent fills, recessive grid and axes) are trivial in SVG and awkward to impose
 * on a library.
 */

import { useId, useState, type ReactNode } from "react";

export const AXIS_FONT = 11;

export interface Margin {
  top: number;
  right: number;
  bottom: number;
  left: number;
}

export const M = (
  top: number,
  right: number,
  bottom: number,
  left: number,
): Margin => ({ top, right, bottom, left });

/* ------------------------------------------------------------------ frame */

export function Figure({
  title,
  subtitle,
  note,
  table,
  children,
}: {
  title: string;
  subtitle?: string;
  note?: string;
  /** The table view. Present on every chart: it is the accessibility fallback and
   *  it discharges the contrast obligation on the aqua series. */
  table?: ReactNode;
  children: ReactNode;
}) {
  const [showTable, setShowTable] = useState(false);
  const id = useId();

  return (
    <figure className="figure" aria-labelledby={`${id}-t`}>
      <div className="figure__head">
        <div>
          <h3 id={`${id}-t`} className="figure__title">
            {title}
          </h3>
          {subtitle && <p className="figure__sub">{subtitle}</p>}
        </div>
        {table && (
          <button
            type="button"
            className="figure__toggle"
            aria-pressed={showTable}
            onClick={() => setShowTable((v) => !v)}
          >
            {showTable ? "Chart" : "Table"}
          </button>
        )}
      </div>

      <div className="figure__body">{showTable && table ? table : children}</div>

      {note && <figcaption className="figure__note">{note}</figcaption>}
    </figure>
  );
}

/* ------------------------------------------------------------------- axes */

export function GridY({
  ticks,
  scale,
  x0,
  x1,
}: {
  ticks: number[];
  scale: (v: number) => number;
  x0: number;
  x1: number;
}) {
  return (
    <g aria-hidden="true">
      {ticks.map((t) => (
        <line
          key={t}
          x1={x0}
          x2={x1}
          y1={scale(t)}
          y2={scale(t)}
          stroke="var(--grid)"
          strokeWidth={1}
          shapeRendering="crispEdges"
        />
      ))}
    </g>
  );
}

export function AxisLeft({
  ticks,
  scale,
  x,
  format = (v: number) => String(v),
}: {
  ticks: number[];
  scale: (v: number) => number;
  x: number;
  format?: (v: number) => string;
}) {
  return (
    <g aria-hidden="true">
      {ticks.map((t) => (
        <text
          key={t}
          x={x - 8}
          y={scale(t)}
          dy="0.32em"
          textAnchor="end"
          fill="var(--ink-muted)"
          fontSize={AXIS_FONT}
          fontFamily="var(--font-mono)"
        >
          {format(t)}
        </text>
      ))}
    </g>
  );
}

export function AxisBottom({
  ticks,
  scale,
  y,
  format = (v: string) => v,
  angled = false,
}: {
  ticks: string[];
  scale: (v: string) => number | undefined;
  y: number;
  format?: (v: string) => string;
  angled?: boolean;
}) {
  return (
    <g aria-hidden="true">
      {ticks.map((t) => {
        const cx = scale(t);
        if (cx === undefined) return null;
        return (
          <text
            key={t}
            x={cx}
            y={y + (angled ? 10 : 16)}
            textAnchor={angled ? "end" : "middle"}
            transform={angled ? `rotate(-35 ${cx} ${y + 10})` : undefined}
            fill="var(--ink-muted)"
            fontSize={AXIS_FONT}
          >
            {format(t)}
          </text>
        );
      })}
    </g>
  );
}

/** The zero/base rule. Slightly stronger than a gridline — a bar's length is read
 *  against it, so it has to be findable. */
export function Baseline({ y, x0, x1 }: { y: number; x0: number; x1: number }) {
  return (
    <line
      x1={x0}
      x2={x1}
      y1={y}
      y2={y}
      stroke="var(--axis)"
      strokeWidth={1}
      shapeRendering="crispEdges"
    />
  );
}

/* ---------------------------------------------------------------- tooltip */

export interface TipState {
  x: number;
  y: number;
  rows: [string, string][];
  title: string;
}

export function Tooltip({ tip, width }: { tip: TipState | null; width: number }) {
  if (!tip) return null;
  // Flip the tooltip to the left of the cursor when it would overflow the frame.
  const flip = tip.x > width - 190;
  return (
    <div
      className="tip"
      style={{
        left: flip ? tip.x - 12 : tip.x + 12,
        top: tip.y,
        transform: flip ? "translate(-100%, -50%)" : "translate(0, -50%)",
      }}
      role="tooltip"
    >
      <div className="tip__title">{tip.title}</div>
      {tip.rows.map(([label, value]) => (
        <div className="tip__row" key={label}>
          <span className="tip__label">{label}</span>
          <span className="tip__value num">{value}</span>
        </div>
      ))}
    </div>
  );
}

/* ----------------------------------------------------------------- legend */

/** A legend is always present for two or more series, so identity is never carried
 *  by colour alone. A single-series chart needs none — its title names it. */
export function Legend({ items }: { items: { label: string; color: string }[] }) {
  return (
    <ul className="legend">
      {items.map((i) => (
        <li key={i.label}>
          <span className="legend__swatch" style={{ background: i.color }} aria-hidden="true" />
          {i.label}
        </li>
      ))}
    </ul>
  );
}

/* ------------------------------------------------------------------ table */

export function Table({
  columns,
  rows,
  align = [],
}: {
  columns: string[];
  rows: (string | number)[][];
  align?: ("l" | "r")[];
}) {
  return (
    <div className="tablewrap">
      <table className="dtable">
        <thead>
          <tr>
            {columns.map((c, i) => (
              <th key={c} className={align[i] === "r" ? "ar" : undefined}>
                {c}
              </th>
            ))}
          </tr>
        </thead>
        <tbody>
          {rows.map((r, ri) => (
            <tr key={ri}>
              {r.map((cell, ci) => (
                <td
                  key={ci}
                  className={[align[ci] === "r" ? "ar" : "", typeof cell === "number" ? "num" : ""]
                    .filter(Boolean)
                    .join(" ")}
                >
                  {cell}
                </td>
              ))}
            </tr>
          ))}
        </tbody>
      </table>
    </div>
  );
}
