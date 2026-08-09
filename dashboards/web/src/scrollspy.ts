/* Which section the rail should highlight, as a pure function.
 *
 * Extracted from the component so it can be tested without a browser. The bug this
 * replaces was invisible in code review and only showed up by scrolling the real
 * page — an IntersectionObserver whose thresholds could never trip because they are
 * fractions of the *target*, and the targets were far taller than the observation
 * band. Logic that can be checked in a test does not get to fail that way twice.
 */

export interface SectionTop {
  id: string;
  /** Distance from the top of the viewport to the top of the section, in px.
   *  Negative once the section has scrolled past the top edge. */
  top: number;
}

export function pickActiveSection(
  sections: SectionTop[],
  /** The reading line — how far down the viewport counts as "here". */
  line: number,
  atBottom = false,
): string {
  if (sections.length === 0) return "";

  // At the very bottom of the page the last section may never reach the line,
  // because there is nothing below it left to scroll. Without this, the final rail
  // entry is unreachable no matter how far you scroll.
  if (atBottom) return sections[sections.length - 1].id;

  // The active section is the last one whose top has crossed the line. Falling back
  // to the first means the rail is never blank above the first boundary.
  let current = sections[0].id;
  for (const section of sections) {
    if (section.top <= line) current = section.id;
  }
  return current;
}
