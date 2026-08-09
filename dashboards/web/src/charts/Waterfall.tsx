/* Waterfall — billed, through each deduction, to net reimbursed.
 *
 * The design decision that matters here is not the form (a waterfall is right for a
 * running total) but the *colour split*. Every deduction is drawn in the recede grey
 * except room-rent excess, which is drawn in the accent. That is the emphasis form,
 * and it encodes the actual finding: of ₹805 Cr not reimbursed, only the room-rent
 * excess is recoverable — it disappears if admission room category matches policy
 * entitlement. Co-pay and contractual exclusions are not money left on the table, and
 * colouring all five deductions alike would imply they were.
 */

import { scaleBand, scaleLinear } from "d3-scale";
import { useState } from "react";
import { AxisLeft, Figure, GridY, M, Table, Tooltip, type TipState } from "./chrome";

export interface WaterfallStep {
  stage: string;
  amount: number;
  kind: string;
  recoverable: boolean;
}

export function Waterfall({
  steps,
  title,
  subtitle,
  note,
  format,
}: {
  steps: WaterfallStep[];
  title: string;
  subtitle?: string;
  note?: string;
  format: (v: number) => string;
}) {
  const [tip, setTip] = useState<TipState | null>(null);
  const width = 720;
  const height = 400;
  const m = M(20, 20, 72, 76);
  const iw = width - m.left - m.right;
  const ih = height - m.top - m.bottom;

  // Walk the running total to find each bar's [start, end].
  let running = 0;
  const bars = steps.map((s) => {
    if (s.kind === "total") {
      const bar = { ...s, y0: 0, y1: s.amount, isTotal: true };
      running = s.amount;
      return bar;
    }
    const y0 = running;
    running += s.amount; // amount is negative for deductions
    return { ...s, y0, y1: running, isTotal: false };
  });

  const top = Math.max(...bars.map((b) => Math.max(b.y0, b.y1)));
  const x = scaleBand<string>().domain(steps.map((s) => s.stage)).range([0, iw]).padding(0.32);
  const y = scaleLinear().domain([0, top * 1.06]).range([ih, 0]).nice();
  const ticks = y.ticks(5);

  return (
    <Figure
      title={title}
      subtitle={subtitle}
      note={note}
      table={
        <Table
          columns={["Stage", "Amount", "Recoverable"]}
          align={["l", "r", "l"]}
          rows={steps.map((s) => [
            s.stage,
            format(Math.abs(s.amount)),
            s.kind === "total" ? "—" : s.recoverable ? "Yes" : "No — contractual",
          ])}
        />
      }
    >
      <div className="plot" onMouseLeave={() => setTip(null)}>
        <svg viewBox={`0 0 ${width} ${height}`} role="img" aria-label={title}>
          <g transform={`translate(${m.left},${m.top})`}>
            <GridY ticks={ticks} scale={y} x0={0} x1={iw} />
            <AxisLeft ticks={ticks} scale={y} x={0} format={(v) => format(v)} />

            {bars.map((b, i) => {
              const bx = x(b.stage) ?? 0;
              const yTop = y(Math.max(b.y0, b.y1));
              const barH = Math.max(2, Math.abs(y(b.y0) - y(b.y1)));
              const fill = b.isTotal
                ? "var(--cat-1)"
                : b.recoverable
                  ? "var(--cat-2)"
                  : "var(--recede)";
              const prev = bars[i - 1];
              return (
                <g
                  key={b.stage}
                  onMouseMove={(e) => {
                    const box = (e.currentTarget.ownerSVGElement as SVGSVGElement)
                      .parentElement!.getBoundingClientRect();
                    setTip({
                      x: e.clientX - box.left,
                      y: e.clientY - box.top,
                      title: b.stage,
                      rows: [
                        ["Amount", format(Math.abs(b.amount))],
                        ...(b.isTotal
                          ? ([] as [string, string][])
                          : ([
                              ["Running total", format(b.y1)],
                              ["Recoverable", b.recoverable ? "Yes" : "No — contractual"],
                            ] as [string, string][])),
                      ],
                    });
                  }}
                >
                  <rect x={bx - 6} y={0} width={x.bandwidth() + 12} height={ih} fill="transparent" />
                  {/* Connector from the previous bar's end, so the running total reads
                      as one continuous quantity being reduced. */}
                  {prev && (
                    <line
                      x1={(x(prev.stage) ?? 0) + x.bandwidth()}
                      x2={bx}
                      y1={y(prev.y1)}
                      y2={y(prev.y1)}
                      stroke="var(--axis)"
                      strokeDasharray="2 3"
                    />
                  )}
                  <rect x={bx} y={yTop} width={x.bandwidth()} height={barH} fill={fill} rx={3} />
                  <text
                    x={bx + x.bandwidth() / 2}
                    y={yTop - 8}
                    textAnchor="middle"
                    fontSize={11}
                    fontFamily="var(--font-mono)"
                    fill="var(--ink-secondary)"
                  >
                    {format(Math.abs(b.amount))}
                  </text>
                </g>
              );
            })}

            <g transform={`translate(0,${ih + 8})`}>
              {steps.map((s) => {
                const cx = (x(s.stage) ?? 0) + x.bandwidth() / 2;
                return (
                  <text
                    key={s.stage}
                    x={cx}
                    y={12}
                    textAnchor="end"
                    transform={`rotate(-28 ${cx} 12)`}
                    fontSize={11}
                    fill="var(--ink-muted)"
                  >
                    {s.stage}
                  </text>
                );
              })}
            </g>
          </g>
        </svg>
        <Tooltip tip={tip} width={width} />
        <ul className="legend">
          <li>
            <span className="legend__swatch" style={{ background: "var(--cat-1)" }} aria-hidden="true" />
            Total
          </li>
          <li>
            <span className="legend__swatch" style={{ background: "var(--cat-2)" }} aria-hidden="true" />
            Recoverable deduction
          </li>
          <li>
            <span className="legend__swatch" style={{ background: "var(--recede)" }} aria-hidden="true" />
            Contractual — not recoverable
          </li>
        </ul>
      </div>
    </Figure>
  );
}
