import { describe, expect, it } from "vitest";
import { pickActiveSection, type SectionTop } from "./scrollspy";

/* The rail's active state, tested at the positions that actually broke it.
 *
 * The original implementation used an IntersectionObserver whose thresholds were
 * fractions of the target element, against sections thousands of pixels tall. It
 * never fired and the rail never moved. None of that was visible in review — only by
 * scrolling. These cases pin the behaviour so it cannot regress silently again.
 */

const LINE = 300; // a third of a 900px viewport

/** Section tops as they would read at a given scroll offset. */
const at = (scrollY: number): SectionTop[] =>
  [
    { id: "overview", top: 0 },
    { id: "clinical", top: 1200 },
    { id: "operational", top: 3400 },
    { id: "financial", top: 5900 },
    { id: "quality", top: 8100 },
  ].map((s) => ({ ...s, top: s.top - scrollY }));

describe("pickActiveSection", () => {
  it("starts on the first section", () => {
    expect(pickActiveSection(at(0), LINE)).toBe("overview");
  });

  it("stays on the first section until the second crosses the line", () => {
    expect(pickActiveSection(at(500), LINE)).toBe("overview");
    expect(pickActiveSection(at(899), LINE)).toBe("overview");
  });

  it("advances exactly when a section top reaches the line", () => {
    // clinical.top = 1200 - 900 = 300, which is the line.
    expect(pickActiveSection(at(900), LINE)).toBe("clinical");
  });

  it("tracks through every section on the way down", () => {
    expect(pickActiveSection(at(1500), LINE)).toBe("clinical");
    expect(pickActiveSection(at(3100), LINE)).toBe("operational");
    expect(pickActiveSection(at(4000), LINE)).toBe("operational");
    expect(pickActiveSection(at(5600), LINE)).toBe("financial");
    expect(pickActiveSection(at(7800), LINE)).toBe("quality");
  });

  it("tracks back up again", () => {
    expect(pickActiveSection(at(7800), LINE)).toBe("quality");
    expect(pickActiveSection(at(5600), LINE)).toBe("financial");
    expect(pickActiveSection(at(0), LINE)).toBe("overview");
  });

  it("selects the last section at the bottom of the page", () => {
    // The real failure this guards: the final section is short enough that its top
    // never reaches the line, because there is nothing below it left to scroll. The
    // rail would stick on the second-to-last entry forever.
    expect(pickActiveSection(at(4000), LINE, true)).toBe("quality");
  });

  it("never returns nothing when scrolled above the first section", () => {
    // Overscroll / rubber-banding puts every top below the line.
    expect(pickActiveSection(at(-200), LINE)).toBe("overview");
  });

  it("handles a missing section element", () => {
    const withMissing: SectionTop[] = [
      { id: "overview", top: -500 },
      { id: "clinical", top: Infinity }, // getElementById returned null
      { id: "operational", top: 100 },
    ];
    expect(pickActiveSection(withMissing, LINE)).toBe("operational");
  });

  it("returns an empty string rather than throwing on no sections", () => {
    expect(pickActiveSection([], LINE)).toBe("");
  });
});
