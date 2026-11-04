"""Builds FraudStream_Report.pdf and FraudStream_Report.docx in the sample report's format.

    python build_report.py            (needs reportlab, python-docx)

Layout copied from the sample: US Letter, 0.8 in side margins, a 2.16 pt page border 18 pt from the
page edge, Times New Roman 12 pt justified at 1.15 lines, underlined 14.3 pt section headings,
grey 9 pt code boxes, tables with a light-blue header row and "Page N" centred in the footer.
"""
import json
import re
from pathlib import Path

from content import COVER, blocks

HERE = Path(__file__).parent
ASSETS = HERE / "assets"
INK = (0x17, 0x17, 0x17)
CODE_FILL = (0xF0, 0xF0, 0xF0)
HEAD_FILL = (0xD9, 0xE0, 0xF3)
PAGE_W, PAGE_H = 612, 792
MARGIN_X, MARGIN_TOP, MARGIN_BOTTOM = 57.6, 50.4, 72
TEXT_W = PAGE_W - 2 * MARGIN_X
FIG_W = 360  # figure width in points, as in the sample

# Cover positions (points from the top of the page), measured from the sample.
COVER_LINES = [  # (text key, size, top)
    ("course0", 28, 152), ("course1", 28, 189), ("activity", 21, 229), ("on", 14, 263),
    ("title0", 20, 287), ("title1", 20, 313), ("submitted", 16, 372),
]


def load_results():
    path = HERE / "results.json"
    return json.loads(path.read_text(encoding="utf-8")) if path.exists() else {}


def cover_text():
    return {"course0": COVER["course"][0], "course1": COVER["course"][1], "activity": COVER["activity"],
            "on": "On", "title0": COVER["title"][0], "title1": COVER["title"][1], "submitted": "Submitted By"}


# ----------------------------------------------------------------------------------- PDF
def build_pdf(items, out):
    from reportlab.lib.colors import Color
    from reportlab.lib.enums import TA_JUSTIFY, TA_LEFT
    from reportlab.lib.styles import ParagraphStyle
    from reportlab.pdfbase import pdfmetrics
    from reportlab.pdfbase.ttfonts import TTFont
    from reportlab.platypus import (BaseDocTemplate, CondPageBreak, Frame, Image, KeepTogether, ListFlowable,
                                    ListItem, NextPageTemplate, PageBreak, PageTemplate, Paragraph, Spacer, Table,
                                    TableStyle)
    from reportlab.lib.fonts import addMapping

    fonts = Path("C:/Windows/Fonts")
    for name, file in [("TNR", "times.ttf"), ("TNR-B", "timesbd.ttf"), ("TNR-I", "timesi.ttf"), ("TNR-BI", "timesbi.ttf")]:
        pdfmetrics.registerFont(TTFont(name, str(fonts / file)))
    addMapping("TNR", 0, 0, "TNR"); addMapping("TNR", 1, 0, "TNR-B"); addMapping("TNR", 0, 1, "TNR-I"); addMapping("TNR", 1, 1, "TNR-BI")

    ink = Color(*(c / 255 for c in INK))
    rgb = lambda t: Color(*(c / 255 for c in t))
    body = ParagraphStyle("body", fontName="TNR", fontSize=12, leading=15.9, alignment=TA_JUSTIFY, textColor=ink, spaceAfter=5.5)
    h1 = ParagraphStyle("h1", fontName="TNR-B", fontSize=14.3, leading=17, textColor=ink, spaceBefore=12, spaceAfter=11)
    h2 = ParagraphStyle("h2", fontName="TNR-B", fontSize=14, leading=16.5, textColor=ink, spaceBefore=8, spaceAfter=6)
    bullet = ParagraphStyle("bullet", parent=body, alignment=TA_JUSTIFY, leading=16, spaceAfter=3)
    code = ParagraphStyle("code", fontName="TNR", fontSize=9, leading=10.3, textColor=ink)
    cell = ParagraphStyle("cell", fontName="TNR", fontSize=9, leading=10.5, textColor=ink)
    cell_b = ParagraphStyle("cellb", parent=cell, fontName="TNR-B")
    caption = ParagraphStyle("caption", parent=body, alignment=TA_LEFT, spaceBefore=6, spaceAfter=4)

    def esc(s):
        s = s.replace("&", "&amp;").replace("<", "&lt;").replace(">", "&gt;")
        return s.replace("&lt;b&gt;", "<b>").replace("&lt;/b&gt;", "</b>")

    def frame_page(canvas, doc):
        canvas.saveState()
        canvas.setFillColor(ink)
        canvas.setLineWidth(2.16)
        canvas.rect(19.08, 19.08, PAGE_W - 38.16, PAGE_H - 38.16, stroke=1, fill=0)
        canvas.setFont("TNR", 11.04)
        canvas.drawCentredString(PAGE_W / 2, PAGE_H - 754, f"Page {doc.page}")
        canvas.restoreState()

    def cover_page(canvas, doc):
        frame_page(canvas, doc)
        canvas.saveState()
        canvas.setFillColor(ink)
        canvas.drawImage(str(ASSETS / "vvce_header.jpeg"), 29.4, PAGE_H - 134.8, width=549.2, height=105.8)
        text = cover_text()
        for key, size, y0 in COVER_LINES:
            canvas.setFont("TNR-B", size)
            canvas.drawCentredString(PAGE_W / 2, PAGE_H - y0 - size * 0.86, text[key])
        canvas.setFont("TNR-B", 12.96)
        for i, (name, usn) in enumerate(COVER["members"]):
            y = PAGE_H - 396 - 17 * i - 11
            canvas.drawString(196, y, name)
            canvas.drawString(344, y, usn)
        canvas.drawImage(str(ASSETS / "ise_logo.jpeg"), 256, PAGE_H - 624, width=100.5, height=100.8)
        canvas.setFont("TNR-B", 14.04)
        canvas.drawCentredString(PAGE_W / 2, PAGE_H - 663, "Under the guidance of")
        canvas.drawCentredString(PAGE_W / 2, PAGE_H - 681, COVER["guide"])
        canvas.setFont("TNR-B", 11.04)
        canvas.drawCentredString(PAGE_W / 2, PAGE_H - 703, COVER["department"])
        canvas.drawCentredString(PAGE_W / 2, PAGE_H - 718, COVER["year"])
        canvas.restoreState()

    doc = BaseDocTemplate(str(out), pagesize=(PAGE_W, PAGE_H), leftMargin=MARGIN_X, rightMargin=MARGIN_X,
                          topMargin=MARGIN_TOP, bottomMargin=MARGIN_BOTTOM,
                          title=f"{COVER['title'][0]} {COVER['title'][1]}", author="FraudStream team")
    frame = Frame(MARGIN_X, MARGIN_BOTTOM, TEXT_W, PAGE_H - MARGIN_TOP - MARGIN_BOTTOM, 0, 0, 0, 0)
    doc.addPageTemplates([PageTemplate("cover", [frame], onPage=cover_page),
                          PageTemplate("body", [frame], onPage=frame_page)])

    story = [NextPageTemplate("body"), PageBreak()]
    for b in items:
        kind = b[0]
        if kind == "h1":
            story += [CondPageBreak(90), Paragraph(f"<u>{esc(b[1])}</u>", h1)]
        elif kind == "h2":
            story += [CondPageBreak(70), Paragraph(esc(b[1]), h2)]
        elif kind == "p":
            story.append(Paragraph(esc(b[1]), body))
        elif kind == "gap":
            story.append(Spacer(1, 12))
        elif kind == "bullets":
            # Bullet 18 pt in, text 36 pt in, as in the sample.
            items_ = [ListItem(Paragraph(f"<b>{esc(bold)}</b> {esc(rest)}", bullet), leftIndent=36, value="\u2022")
                      for bold, rest in b[1]]
            story += [ListFlowable(items_, bulletType="bullet", start="\u2022", leftIndent=36, bulletFontName="TNR",
                                   bulletFontSize=12, bulletOffsetY=0, bulletDedent=18), Spacer(1, 4)]
        elif kind == "code":
            lines = "<br/>".join(esc(l).replace(" ", "&nbsp;") for l in b[1])
            t = Table([[Paragraph(lines, code)]], colWidths=[TEXT_W])
            t.setStyle(TableStyle([("BACKGROUND", (0, 0), (-1, -1), rgb(CODE_FILL)),
                                   ("LEFTPADDING", (0, 0), (-1, -1), 5.5), ("TOPPADDING", (0, 0), (-1, -1), 1.5),
                                   ("BOTTOMPADDING", (0, 0), (-1, -1), 4)]))
            # Keep a code box on the same page as the sentence that introduces it.
            lead = [story.pop()] if story and isinstance(story[-1], Paragraph) else []
            story += [KeepTogether(lead + [Spacer(1, 4), t]), Spacer(1, 10)]
        elif kind == "table":
            header, rows, widths = b[1], b[2], b[3]
            data = [[Paragraph(esc(h), cell_b) for h in header]] + [[Paragraph(esc(str(c)), cell) for c in r] for r in rows]
            t = Table(data, colWidths=widths, repeatRows=1)
            t.setStyle(TableStyle([("GRID", (0, 0), (-1, -1), 0.48, ink), ("BACKGROUND", (0, 0), (-1, 0), rgb(HEAD_FILL)),
                                   ("VALIGN", (0, 0), (-1, -1), "TOP"), ("LEFTPADDING", (0, 0), (-1, -1), 4),
                                   ("RIGHTPADDING", (0, 0), (-1, -1), 4), ("TOPPADDING", (0, 0), (-1, -1), 1),
                                   ("BOTTOMPADDING", (0, 0), (-1, -1), 1.5)]))
            story += [Spacer(1, 4), t, Spacer(1, 12)]
        elif kind == "figure":
            path = HERE / b[2]
            if path.exists():
                from reportlab.lib.utils import ImageReader
                w, h = ImageReader(str(path)).getSize()
                img = Image(str(path), width=FIG_W, height=FIG_W * h / w, hAlign="LEFT")
            else:
                img = Paragraph("[screenshot missing: " + esc(b[2]) + "]", body)
            story.append(KeepTogether([Paragraph(esc(b[1]), caption), img, Spacer(1, 8)]))
    doc.build(story)


# ---------------------------------------------------------------------------------- DOCX
def build_docx(items, out):
    from docx import Document
    from docx.enum.section import WD_SECTION
    from docx.enum.table import WD_TABLE_ALIGNMENT
    from docx.enum.text import WD_ALIGN_PARAGRAPH, WD_LINE_SPACING, WD_TAB_ALIGNMENT
    from docx.oxml import OxmlElement
    from docx.oxml.ns import qn
    from docx.shared import Pt, RGBColor

    ink = RGBColor(*INK)
    doc = Document()
    sec = doc.sections[0]
    sec.page_width, sec.page_height = Pt(PAGE_W), Pt(PAGE_H)
    sec.left_margin = sec.right_margin = Pt(MARGIN_X)
    sec.top_margin, sec.bottom_margin = Pt(MARGIN_TOP), Pt(MARGIN_BOTTOM)
    sec.footer_distance = Pt(30)

    # Page border on every page, 18 pt from the page edge.
    borders = OxmlElement("w:pgBorders")
    borders.set(qn("w:offsetFrom"), "page")
    for side in ("top", "left", "bottom", "right"):
        e = OxmlElement(f"w:{side}")
        e.set(qn("w:val"), "single"); e.set(qn("w:sz"), "18"); e.set(qn("w:space"), "18"); e.set(qn("w:color"), "000000")
        borders.append(e)
    sec._sectPr.append(borders)

    def font(run_or_style, size=None, bold=None, underline=None):
        f = run_or_style.font
        f.name = "Times New Roman"
        f.color.rgb = ink
        if size: f.size = Pt(size)
        if bold is not None: f.bold = bold
        if underline is not None: f.underline = underline
        rpr = run_or_style.element.get_or_add_rPr() if hasattr(run_or_style.element, "get_or_add_rPr") else None
        if rpr is not None:
            rfonts = rpr.find(qn("w:rFonts"))
            if rfonts is None:
                rfonts = OxmlElement("w:rFonts"); rpr.insert(0, rfonts)
            for a in ("w:ascii", "w:hAnsi", "w:cs", "w:eastAsia"):
                rfonts.set(qn(a), "Times New Roman")
            for a in ("w:asciiTheme", "w:hAnsiTheme", "w:cstheme", "w:eastAsiaTheme"):
                rfonts.attrib.pop(qn(a), None)

    normal = doc.styles["Normal"]
    font(normal, 12)
    pf = normal.paragraph_format
    pf.space_before, pf.space_after = Pt(0), Pt(6)
    pf.line_spacing_rule, pf.line_spacing = WD_LINE_SPACING.MULTIPLE, 1.15
    for name, size, before, after, under in (("Heading 1", 14.3, 12, 10, True), ("Heading 2", 14, 10, 6, False)):
        st = doc.styles[name]
        font(st, size, True, under)
        st.font.italic = False
        st.paragraph_format.space_before, st.paragraph_format.space_after = Pt(before), Pt(after)
        st.paragraph_format.keep_with_next = True
        st.paragraph_format.line_spacing = 1.0
    lb = doc.styles["List Bullet"]
    font(lb, 12)
    lb.paragraph_format.space_after = Pt(3)
    lb.paragraph_format.left_indent, lb.paragraph_format.first_line_indent = Pt(36), Pt(-18)

    def shade(el, fill):
        shd = OxmlElement("w:shd")
        shd.set(qn("w:val"), "clear"); shd.set(qn("w:color"), "auto"); shd.set(qn("w:fill"), "%02X%02X%02X" % fill)
        el.append(shd)

    def runs(par, text, size=None):
        for i, part in enumerate(re.split(r"</?b>", text)):
            if part:
                r = par.add_run(part)
                font(r, size, bold=(i % 2 == 1) or None)

    # Footer: "Page N", centred.
    fp = sec.footer.paragraphs[0]
    fp.alignment = WD_ALIGN_PARAGRAPH.CENTER
    r = fp.add_run("Page "); font(r, 11)
    r = fp.add_run()
    for tag, attr in (("w:fldChar", "begin"), ("w:instrText", None), ("w:fldChar", "end")):
        e = OxmlElement(tag)
        if attr: e.set(qn("w:fldCharType"), attr)
        else: e.set(qn("xml:space"), "preserve"); e.text = "PAGE"
        r._r.append(e)
    font(r, 11)

    # Cover page.
    def centred(text, size, before=0, after=0):
        p = doc.add_paragraph()
        p.alignment = WD_ALIGN_PARAGRAPH.CENTER
        p.paragraph_format.space_before, p.paragraph_format.space_after = Pt(before), Pt(after)
        p.paragraph_format.line_spacing = 1.0
        runs(p, text); [font(r, size, True) for r in p.runs]
        return p

    p = doc.add_paragraph()
    p.paragraph_format.left_indent = Pt(29.4 - MARGIN_X)
    p.paragraph_format.right_indent = Pt(29.4 - MARGIN_X)
    p.paragraph_format.space_before = Pt(0)
    p.paragraph_format.line_spacing = 1.0
    p.add_run().add_picture(str(ASSETS / "vvce_header.jpeg"), width=Pt(549.2))
    text = cover_text()
    centred(text["course0"], 28, 12); centred(text["course1"], 28)
    centred(text["activity"], 21, 6, 4); centred(text["on"], 14, 0, 4)
    centred(text["title0"], 20); centred(text["title1"], 20)
    centred(text["submitted"], 16, 32, 2)
    for name, usn in COVER["members"]:
        p = doc.add_paragraph()
        p.paragraph_format.left_indent = Pt(196 - MARGIN_X)
        p.paragraph_format.space_after, p.paragraph_format.line_spacing = Pt(2), 1.0
        p.paragraph_format.tab_stops.add_tab_stop(Pt(344 - MARGIN_X), WD_TAB_ALIGNMENT.LEFT)
        r = p.add_run(f"{name}\t{usn}"); font(r, 13, True)
    p = doc.add_paragraph()
    p.alignment = WD_ALIGN_PARAGRAPH.CENTER
    p.paragraph_format.space_before = Pt(232 - 17 * len(COVER["members"]) - 18 * 2)
    p.add_run().add_picture(str(ASSETS / "ise_logo.jpeg"), width=Pt(100.5))
    centred("Under the guidance of", 14, 20); centred(COVER["guide"], 14, 0, 6)
    centred(COVER["department"], 11); centred(COVER["year"], 11)
    doc.paragraphs[-1].runs[-1].add_break(__import__("docx.enum.text", fromlist=["WD_BREAK"]).WD_BREAK.PAGE)

    for b in items:
        kind = b[0]
        if kind == "h1":
            doc.add_paragraph(b[1], style="Heading 1")
            font(doc.paragraphs[-1].runs[0], 14.3, True, True)
        elif kind == "h2":
            doc.add_paragraph(b[1], style="Heading 2")
            font(doc.paragraphs[-1].runs[0], 14, True, False)
        elif kind == "p":
            p = doc.add_paragraph(); p.alignment = WD_ALIGN_PARAGRAPH.JUSTIFY; runs(p, b[1])
        elif kind == "gap":
            p = doc.add_paragraph(); p.paragraph_format.space_after = Pt(0); p.paragraph_format.line_spacing = 0.6
        elif kind == "bullets":
            for bold, rest in b[1]:
                p = doc.add_paragraph(style="List Bullet")
                # Direct indents win over the list numbering's own indents.
                p.paragraph_format.left_indent, p.paragraph_format.first_line_indent = Pt(36), Pt(-18)
                p.alignment = WD_ALIGN_PARAGRAPH.JUSTIFY
                r = p.add_run(bold + " "); font(r, 12, True)
                r = p.add_run(rest); font(r, 12)
        elif kind == "code":
            for i, line in enumerate(b[1]):
                p = doc.add_paragraph()
                p.paragraph_format.space_after = Pt(6 if i == len(b[1]) - 1 else 0)
                p.paragraph_format.space_before = Pt(4 if i == 0 else 0)
                p.paragraph_format.line_spacing = 1.0
                p.paragraph_format.left_indent = Pt(0)
                shade(p._p.get_or_add_pPr(), CODE_FILL)
                r = p.add_run(line); font(r, 9)
        elif kind == "table":
            header, rows, widths = b[1], b[2], b[3]
            t = doc.add_table(rows=1 + len(rows), cols=len(header))
            t.style = doc.styles["Table Grid"]
            t.alignment = WD_TABLE_ALIGNMENT.LEFT
            for ri, values in enumerate([header] + rows):
                for ci, v in enumerate(values):
                    c = t.cell(ri, ci)
                    c.width = Pt(widths[ci])
                    p = c.paragraphs[0]
                    p.paragraph_format.space_after, p.paragraph_format.line_spacing = Pt(0), 1.0
                    r = p.add_run(str(v)); font(r, 9, ri == 0)
                    if ri == 0:
                        shade(c._tc.get_or_add_tcPr(), HEAD_FILL)
            doc.add_paragraph().paragraph_format.space_after = Pt(2)
        elif kind == "figure":
            p = doc.add_paragraph(); runs(p, b[1])
            p.paragraph_format.keep_with_next = True
            path = HERE / b[2]
            if path.exists():
                doc.add_paragraph().add_run().add_picture(str(path), width=Pt(FIG_W))
            else:
                doc.add_paragraph(f"[screenshot missing: {b[2]}]")
    doc.core_properties.title = f"{COVER['title'][0]} {COVER['title'][1]}"
    doc.core_properties.author = "FraudStream team"
    doc.save(str(out))


if __name__ == "__main__":
    results = load_results()
    defaults = {k: "" for k in ("session", "total", "normal", "suspicious", "fraud", "invalid", "fraud_rate",
                                "flink_latency", "e2e_latency", "checkpoints", "recovery", "duplicates")}
    items = blocks({**defaults, **results})
    build_pdf(items, HERE / "FraudStream_Report.pdf")
    build_docx(items, HERE / "FraudStream_Report.docx")
    print("wrote FraudStream_Report.pdf and FraudStream_Report.docx")
