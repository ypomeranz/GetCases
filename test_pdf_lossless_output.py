"""Saving and printing keep the scan's own text layer.

The exported file used to be a picture of each page and nothing else — no
fonts, no text, no annotations.  That costs more than searchability: handed a
PDF with no text in it at all, a reader is entitled to conclude there is text
to be recovered, and older Acrobat duly runs its own OCR over the pages and
then prints the "suspect word" highlighting it produces, all over the opinion.

So the pages are cropped by tightening their boxes — lossless, and the text
comes with them — and a redacted case.law scan's black bars are painted out
*on the page* rather than on a picture of it, with the running head lettered
back on as real text.

These exercise the real pdfium code against a stand-in for a case.law scan: a
page picture, an invisible OCR layer over it, and black bars where West's
headnotes were.
"""

import io
import os
import tempfile
import unittest

try:
    import pypdfium2 as pdfium
    from PIL import Image, ImageDraw, ImageFont
    from reportlab.lib.utils import ImageReader
    from reportlab.pdfgen import canvas as rl_canvas
    from reportlab.lib.pagesizes import letter
    _READY = True
except ImportError:  # pragma: no cover - exercised on a bare checkout
    _READY = False

if _READY:
    import courtlistener_gui as g


LINES = [
    "FRIGALIMENT IMPORTING CO. v. B. N. S. INTERNATIONAL SALES CORP.",
    "",
    "   The issue is, what is chicken?  Plaintiff says chicken means a",
    "young chicken, suitable for broiling and frying.  Defendant says",
    "chicken means any bird of that genus that meets contract",
    "specifications on weight and quality, including what it calls stewing",
    "chicken and plaintiff pejoratively terms fowl.",
]


def _serif(size: int):
    for path in ("/usr/share/fonts/truetype/dejavu/DejaVuSerif.ttf",
                 "/usr/share/fonts/truetype/liberation/LiberationSerif-Regular.ttf"):
        if os.path.exists(path):
            return ImageFont.truetype(path, size)
    return ImageFont.load_default()


def _page_picture(page: int, bars: bool) -> "Image.Image":
    img = Image.new("RGB", (1700, 2200), "white")
    d = ImageDraw.Draw(img)
    font = _serif(30)
    y = 200
    if bars:
        for _ in range(3):                       # West headnotes, blacked out
            d.rectangle([200, y, 1500, y + 70], fill=(0, 0, 0))
            y += 90
        d.rectangle([560, 110, 1140, 150], fill=(0, 0, 0))     # running head
    for line in LINES:
        d.text((200, y), line, font=font, fill=(20, 20, 20))
        y += 44
    d.text((820, 2080), str(115 + page), font=font, fill=(20, 20, 20))
    return img


def _redacted_scan(pages: int = 2, bars: bool = True) -> bytes:
    """A stand-in for a static.case.law scan."""
    buf = io.BytesIO()
    c = rl_canvas.Canvas(buf, pagesize=letter)
    for p in range(pages):
        c.drawImage(ImageReader(_page_picture(p, bars)), 0, 0,
                    width=612, height=792)
        t = c.beginText()
        t.setTextRenderMode(3)                   # the invisible OCR layer
        t.setFont("Times-Roman", 11)
        y = 792 - 160
        for line in LINES:
            t.setTextOrigin(72, y)
            t.textLine(line)
            y -= 16
        c.drawText(t)
        c.showPage()
    c.save()
    return buf.getvalue()


def _text_of(path: str, page: int = 0) -> str:
    doc = pdfium.PdfDocument(path)
    tp = doc[page].get_textpage()
    try:
        return tp.get_text_range()
    finally:
        tp.close()


def _dark_pixels(path: str, page: int = 0, below: int = 40) -> int:
    doc = pdfium.PdfDocument(path)
    img = doc[page].render(scale=2).to_pil().convert("L")
    return sum(1 for v in img.convert('L').tobytes() if v < below)


@unittest.skipUnless(_READY, "needs pypdfium2, Pillow and reportlab")
class LosslessOutputTests(unittest.TestCase):
    """_paint_out_pdf_redactions and the crop, on a scan with an OCR layer."""

    @classmethod
    def setUpClass(cls):
        cls.data = _redacted_scan()
        cls.frac = [(0.10, 0.04, 0.90, 0.96)] * 2

    def setUp(self):
        fh = tempfile.NamedTemporaryFile(suffix=".pdf", delete=False)
        fh.close()
        self.path = fh.name
        self.addCleanup(lambda: os.path.exists(self.path)
                        and os.unlink(self.path))

    def _plan(self, header: str = ""):
        """What the pane's detector would report, computed the same way."""
        plan = []
        doc = pdfium.PdfDocument(io.BytesIO(self.data))
        for i in range(len(doc)):
            page = doc[i]
            img = page.render(scale=110 / 72).to_pil().convert("RGB")
            tp = page.get_textpage()
            text = tp.get_text_range()
            tp.close()
            boxes = g._whiten_pdf_redactions(img)
            W, H = img.size
            head = g._header_redaction_box(boxes, img.size) if header else None
            plan.append({
                "boxes": tuple((l / W, t / H, r / W, b / H)
                               for l, t, r, b in boxes),
                "head": (head[0] / W, head[1] / H, head[2] / W, head[3] / H)
                        if head else None,
                "drop": g._page_fully_redacted(text, boxes, img.size),
            })
        return plan

    def test_the_fixture_really_is_a_scan_with_an_ocr_layer(self):
        doc = pdfium.PdfDocument(io.BytesIO(self.data))
        self.assertTrue(g._page_has_scan_background(doc[0]))
        tp = doc[0].get_textpage()
        self.assertGreater(tp.count_chars(), 100)
        tp.close()

    def test_a_full_page_picture_is_recognised_as_a_scan(self):
        # The object-bounds accessor has been called two things by pypdfium2;
        # asking for the wrong one used to leave this test never firing.
        doc = pdfium.PdfDocument(io.BytesIO(self.data))
        for obj in doc[0].get_objects():
            bounds = g._pdf_object_bounds(obj)
            if bounds is not None:
                self.assertEqual(len(bounds), 4)
                break
        else:
            self.fail("no page object reported its bounds")

    def test_painting_out_the_bars_keeps_the_text(self):
        out, _kept = g._paint_out_pdf_redactions(self.data, self._plan())
        with open(self.path, "wb") as fh:
            fh.write(out)
        self.assertIn("pejoratively terms fowl", _text_of(self.path))

    def test_and_actually_removes_them(self):
        before = _dark_pixels_bytes(self.data)
        out, _kept = g._paint_out_pdf_redactions(self.data, self._plan())
        with open(self.path, "wb") as fh:
            fh.write(out)
        self.assertLess(_dark_pixels(self.path), before / 4)

    def test_the_running_head_is_lettered_back_on_as_real_text(self):
        out, _kept = g._paint_out_pdf_redactions(
            self.data, self._plan("190 F. Supp. 116"),
            header_text="190 F. Supp. 116")
        with open(self.path, "wb") as fh:
            fh.write(out)
        self.assertIn("190 F. Supp. 116", _text_of(self.path))

    def test_a_page_the_redaction_emptied_is_dropped(self):
        plan = self._plan()
        plan[0]["drop"] = True
        out, kept = g._paint_out_pdf_redactions(
            self.data, plan, [(0, 0, 1, 1), (0.1, 0.1, 0.9, 0.9)])
        doc = pdfium.PdfDocument(io.BytesIO(out))
        self.assertEqual(len(doc), 1)
        self.assertEqual(kept, [(0.1, 0.1, 0.9, 0.9)])

    def test_a_scan_is_cropped_to_the_viewers_ink_not_its_ocr_layer(self):
        # The OCR glyphs sit wherever the recognizer put them, which is not
        # the printed page's margins; cropping to them throws the scan away.
        out = g._crop_pdf_to_content(self.data, frac_boxes=self.frac)
        doc = pdfium.PdfDocument(io.BytesIO(out))
        width, height = doc[0].get_size()
        self.assertGreater(width, 400)
        self.assertGreater(height, 600)

    def test_a_page_with_no_ink_box_is_left_at_full_size(self):
        out = g._crop_pdf_to_content(self.data, frac_boxes=[None, None])
        doc = pdfium.PdfDocument(io.BytesIO(out))
        self.assertEqual(tuple(round(v) for v in doc[0].get_size()), (612, 792))

    def test_the_crop_keeps_every_word(self):
        out = g._crop_pdf_to_content(self.data, frac_boxes=self.frac)
        with open(self.path, "wb") as fh:
            fh.write(out)
        self.assertIn("INTERNATIONAL SALES CORP.", _text_of(self.path))

    def test_the_written_file_is_never_a_bare_picture(self):
        # What sends older Acrobat off to OCR the page itself.
        out, _kept = g._paint_out_pdf_redactions(
            self.data, self._plan("190 F. Supp. 116"),
            header_text="190 F. Supp. 116")
        with open(self.path, "wb") as fh:
            fh.write(g._crop_pdf_to_content(out, frac_boxes=self.frac))
        doc = pdfium.PdfDocument(self.path)
        tp = doc[0].get_textpage()
        try:
            self.assertGreater(tp.count_chars(), 100)
        finally:
            tp.close()


def _dark_pixels_bytes(data: bytes, page: int = 0, below: int = 40) -> int:
    doc = pdfium.PdfDocument(io.BytesIO(data))
    img = doc[page].render(scale=2).to_pil().convert("L")
    return sum(1 for v in img.convert('L').tobytes() if v < below)


if __name__ == "__main__":
    unittest.main()
