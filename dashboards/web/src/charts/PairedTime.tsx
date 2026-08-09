/* Two stacked panels over one shared time axis.
 *
 * This replaces a dual-axis chart — volume as bars on the left scale, readmission
 * rate as a line on a right scale. Dual-axis is the single most common charting
 * mistake, and the reason is specific: the point where the two series appear to
 * cross, diverge or "track each other" is an artefact of two independently chosen
 * scales. Nudge one axis range and the apparent relationship inverts. Readers draw
 * causal conclusions from a coincidence of scaling.
 *
 * Two panels sharing an x-axis show the same two series honestly. Comparing them
 * costs the reader one vertical saccade and tells no lies.
 */

import { scaleLinear, scalePoint } from "d3-scale";
import { line as d3line, curveMonotoneX } from "d3-shape";
import { useState } from "react";
import { AxisLeft, Baseline, Figure, GridY, M, Table, Tooltip, type TipState } from "./chrome";

export interface TimePoint {
  t: string;
  a: number;
  b: number | null;
}

export function PairedTime({
  points,
  title,
  subtitle,
  note,
  aLabel,
  bLabel,
  formatA,
  formatB,
  formatT,
}: {
  points: TimePoint[];
  title: string;
  subtitle?: string;
  note?: string;
  aLabel: string;
  bLabel: string;
  formatA: (v: number) => string;
  formatB: (v: number) => string;
  formatT: (t: string) => string;
}) {
  const [tip, setTip] = useState<TipState | null>(null);
  const width = 720;
  const panelH = 132;
  const gap = 26;
  const m = M(14, 20, 40, 60);
  const iw = width - m.left - m.right;
  const height = m.top + panelH * 2 + gap + m.bottom;

  const x = scalePoint<string>()
    .domain(points.map((p) => p.t))
    .range([0, iw]);

  const yA = scaleLinear()
    .domain([0, Math.max(...points.map((p) => p.a)) * 1.1])
    .range([panelH, 0])
    .nice();
  const bVals = points.map((p) => p.b).filter((v): v is number => v !== null);
  const yB = scaleLinear()
    .domain([Math.min(...bVals) * 0.9, Math.max(...bVals) * 1.05])
    .range([panelH, 0])
    .nice();

  const bw = Math.max(3, (iw / points.length) * 0.6);
  const pathB = d3line<TimePoint>()
    .defined((p) => p.b !== null)
    .x((p) => x(p.t) ?? 0)
    .y((p) => yB(p.b as number))
    .curve(curveMonotoneX)(points);

  // Label roughly every sixth month so the axis stays legible at 720px.
  const tickEvery = Math.max(1, Math.round(points.length / 6));

  return (
    <Figure
      title={title}
      subtitle={subtitle}
      note={note}
      table={
        <Table
          columns={["Month", aLabel, bLabel]}
          align={["l", "r", "r"]}
          rows={points.map((p) => [
            formatT(p.t),
            formatA(p.a),
            p.b === null ? "—" : formatB(p.b),
          ])}
        />
      }
    >
      <div className="plot" onMouseLeave={() => setTip(null)}>
        <svg viewBox={`0 0 ${width} ${height}`} role="img" aria-label={title}>
          <g transform={`translate(${m.left},${m.top})`}>
            {/* Panel A — volume */}
            <text x={0} y={-2} fontSize={11} fill="var(--ink-muted)">
              {aLabel}
            </text>
            <GridY ticks={yA.ticks(3)} scale={yA} x0={0} x1={iw} />
            <AxisLeft ticks={yA.ticks(3)} scale={yA} x={0} format={formatA} />
            <Baseline y={panelH} x0={0} x1={iw} />
            {points.map((p) => {
              const cx = (x(p.t) ?? 0) - bw / 2;
              const h = panelH - yA(p.a);
              return (
                <rect
                  key={p.t}
                  x={cx}
                  y={yA(p.a)}
                  width={bw}
                  height={Math.max(1, h)}
                  fill="var(--cat-1)"
                  rx={2}
                />
              );
            })}

            {/* Panel B — rate, on its own scale in its own panel */}
            <g transform={`translate(0,${panelH + gap})`}>
              <text x={0} y={-2} fontSize={11} fill="var(--ink-muted)">
                {bLabel}
              </text>
              <GridY ticks={yB.ticks(3)} scale={yB} x0={0} x1={iw} />
              <AxisLeft ticks={yB.ticks(3)} scale={yB} x={0} format={formatB} />
              <Baseline y={panelH} x0={0} x1={iw} />
              {pathB && (
                <path d={pathB} fill="none" stroke="var(--cat-2)" strokeWidth={2} strokeLinejoin="round" />
              )}
              {points.map((p) =>
                p.b === null ? null : (
                  <circle key={p.t} cx={x(p.t) ?? 0} cy={yB(p.b)} r={2.5} fill="var(--cat-2)" />
                ),
              )}
            </g>

            {/* Shared axis */}
            <g transform={`translate(0,${panelH * 2 + gap})`}>
              {points.map((p, i) =>
                i % tickEvery === 0 ? (
                  <text
                    key={p.t}
                    x={x(p.t) ?? 0}
                    y={18}
                    textAnchor="middle"
                    fontSize={11}
                    fill="var(--ink-muted)"
                  >
                    {formatT(p.t)}
                  </text>
                ) : null,
              )}
            </g>

            {/* One hover band per month, spanning both panels — the crosshair that
                makes comparison across the two panels cheap. */}
            {points.map((p) => (
              <rect
                key={`hit-${p.t}`}
                x={(x(p.t) ?? 0) - iw / points.length / 2}
                y={-m.top}
                width={iw / points.length}
                height={height}
                fill="transparent"
                onMouseMove={(e) => {
                  const box = (e.currentTarget.ownerSVGElement as SVGSVGElement)
                    .parentElement!.getBoundingClientRect();
                  setTip({
                    x: e.clientX - box.left,
                    y: e.clientY - box.top,
                    title: formatT(p.t),
                    rows: [
                      [aLabel, formatA(p.a)],
                      [bLabel, p.b === null ? "—" : formatB(p.b)],
                    ],
                  });
                }}
              />
            ))}
          </g>
        </svg>
        <Tooltip tip={tip} width={width} />
      </div>
    </Figure>
  );
}
