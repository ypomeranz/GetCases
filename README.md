CourtListener GUI – Case Law & Legal Research Tool - (Code and this readme file created by AI)

A desktop application (Tkinter) that searches U.S. case law, statutes, regulations, and historical legal materials. It pulls opinions from **CourtListener** and **Google Scholar**, and provides in‑app viewers for:

- Federal & state case law (Supreme Court, circuit courts, district courts, state appellate courts)
- U.S. Code, Code of Federal Regulations (CFR)
- Federal Rules (Civil, Criminal, Evidence, Appellate, Bankruptcy)
- U.S. Constitution (full text, searchable)
- California & Florida statutes (more states coming)
- U.S. Statutes at Large (official PDFs)
- English Reports (pre‑1865 cases from CommonLII)
- Federal Cases (pre‑1880 lower federal opinions cited by case number, resolved via CourtListener)
- Supreme Court case details (Oyez – summaries, vote splits, oral argument audio)

---

## Getting Started

### 1. Install Python 3.9+ and dependencies

```bash
# Clone or download the source code, then install required packages:
pip install requests beautifulsoup4 pypdfium2 Pillow pynput curl_cffi browser_cookie3 playwright

# For English Reports PDF downloads via Playwright (optional but recommended):
playwright install chromium
Note: The app will prompt you to install missing packages on first run.

2. Get a CourtListener API Token
Go to CourtListener.com and create a free account.

Once logged in, visit your API settings page and copy your API token.

Launch the app – with no token saved, it opens a setup dialog on startup that explains where to get one, offers a “Get a token…” button, and checks the token against the API before saving it. “Skip for now” dismisses it (everything except CourtListener still works), and “Don’t ask again” stops it from reappearing.
You can enter or change the token at any time via Settings → API Token… in the menu bar.

The token is stored locally (~/.config/courtlistener/config.json) and is used for all CourtListener API requests. Setting COURTLISTENER_TOKEN in the environment overrides the saved token and suppresses the startup prompt.

3. Run the application
bash
python courtlistener_gui.py
On first start, the main window will be hidden – it runs in the background.

Press Ctrl+Space (or Cmd+Space on macOS) to open a quick‑search popup.

The popup ranks its results by how close each case’s **name** is to what you typed, and drops the rest – which is right for a caption (“Roe v. Wade”) and wrong for a subject. So when that ranking leaves nothing (no rows, or a single stray one) the query is read as a **phrase** instead: Google Scholar’s own first results page is shown as it stands, in Scholar’s relevance order, and the status line says so. A query carrying a reporter citation is never re‑read this way – it names one case, so finding none of it is a real miss rather than something the ranking did.

Type s + Enter in the terminal to show the main window, or q + Enter to quit.

How to Search
Main Search Window
Enter a search query (e.g., "Roe v. Wade", "Fifth Amendment", "42 U.S.C. § 1983").

Optionally filter by:

Court – click the “Courts: All ▾” button to select specific courts.

Date range – use the “Filed from:” and “to:” fields (YYYY‑MM‑DD).

Max results – number of results per page (default 20).

Press Search or hit Enter.

Results appear in the left treeview; click a row to preview the snippet.

Double‑click a row to download the opinion as PDF (or .txt if no PDF is available).

Click **Scholar** to fetch the full opinion from Google Scholar (often richer than CourtListener’s HTML); the button on the Scholar view reads **CourtListener** and switches back. Under a PDF both read **Text**.

In the opinion text, each separate writing is marked two ways and no more: a light tint behind it (red for a dissent, green for a concurrence, neutral grey where the role isn’t known) and a colour‑coded strip down the right naming every part – click one to jump to it. The strip has room for a surname at most, so resting on a part names it in full – the kind of writing, who wrote it, and the reporter page it starts on (“Dissent — Rehnquist — p. 171”), the same answer the rail beside a PDF’s scrollbar gives. A writing that begins at the very end of the opinion keeps its place on that strip rather than being drawn off the bottom of it.

**Side panel** (or press **s**) opens the case‑details panel beside the opinion: the window widens by the panel’s width so the text keeps its measure, and narrows again when you close it. The opinion is held at its width throughout, so nothing about it re‑wraps or repaints – the panel simply appears next to text that has not moved. The window may end up wider than the screen – move it left if you like. A maximized window has nowhere to grow, so there the panel takes its width from the text as before.

**PDF** shows the official scan in place of the text, and **Text** brings the text back at the passage the PDF was left on. Tick **Window → View PDF in Separate Window** and the scan instead opens in a small window of its own, on the left of the desktop – to the reader’s left where there is room, to its right where there is not – the page under one thin strip carrying small **save** and **print** icons, the zoom controls (**−**, **+**, **Fit**, and Ctrl/Cmd +, −, 0) and the **T** button described below, and nothing else: the opinion text stays on screen beside it. The window is named for the case in Bluebook form, cited to the reporter the pages on screen actually print rather than to every parallel reporter the case carries; for a case you reached by following a citation the name is filled in from the opinion text fetched in the background. Citations in the page are still clickable, Ctrl/Cmd+F still searches it, and Save (Ctrl/Cmd+S), Print (Ctrl/Cmd+P) and Close (Ctrl/Cmd+W) are also on the strip’s right‑click menu. Saved and printed files are named the same way. The setting is remembered between sessions.

When the opinion in that window carries **separate writings**, a slim rail appears just inside the scrollbar mapping them: each part covers the stretch of the document it occupies, in its own color (blue for the Court’s opinion, green for a concurrence, red for a dissent, grey for a syllabus), with a solid marker on its first line. Dragging the scrollbar you can see how far down the dissent starts; hovering names the part and its page, and clicking jumps straight to it. The parts come from the same detector the **Side panel** parts list uses, and the rail appears only for an opinion that actually has a concurrence, dissent or other separate writing – a lone majority gets no rail.

**T on the strip reads the opinion instead of the scan.** The text loads into the same window in place of the pages – the copy fetched in the background while you were looking at the scan, so there is nothing to wait for – and the button becomes **P**, which puts the pages back. Switching either way is instant: neither surface is thrown away, and the scan keeps its zoom while the text is showing.

**Each switch lands where you were.** T opens the text at the passage the pages were open at, and P opens the scan at the page the text was scrolled to, using the same opinion-to-pages alignment the case window's own PDF/Text switch uses. Working that alignment out takes a moment in the background, so a very early first press may land at the top instead – it catches up as soon as the alignment is ready, unless you have started scrolling, in which case it leaves you where you are.

The text runs the full width of the window: instead of the labelled parts strip the case window uses, the separate writings are marked on the same slim colour rail beside the scrollbar the scan uses (click a band to jump to that part, rest on one to see whose writing it is), and the reporter page numbers keep a narrow gutter on the left, cut to exactly four figures across — as far as any reporter in ordinary use runs — with the type shrinking only for a page number longer than that. Both page tracks share that one column: where a U.S. Reports page recovered from the scan falls on the same line as the source’s own star page, the U.S. page keeps the line and the other moves to the line above, or below when the line above is taken. The strip follows what it is showing – **−** and **+** size the type rather than zooming the page (the readout says points instead of percent), **Fit** gives its place to a **Copy** menu offering the same copy styles the case window carries, the save icon writes the opinion out as RTF, and the print icon typesets it with LaTeX. Ctrl/Cmd+F searches whichever surface is on screen.

A PDF viewer is a window in its own right: **closing the opinion text leaves it open**, and Save, Print, **T** and the citations on its pages keep working.

**Print asks which printer.** On macOS the system’s own print dialog comes up. Everywhere else the app lists the machine’s printers in a small dialog of its own, with a way straight into the chosen printer’s **own settings** – where duplex, paper size and quality live, since those belong to the printer and not to us – and, where it can honour the request itself (CUPS), a **Print on both sides** box so the common case needs no detour. If neither can be done, the document opens in the system’s PDF viewer to print from there, as it always did.

What Print sends is the pages **as the viewer shows them**: each one cropped to its content and re‑centred on a clean page with an even margin, a redacted case.law scan whitened so the black bars don’t drink ink, and the running head re‑lettered with the citation the redaction took off it. **Save writes that same file** – so what you file away is what you would have printed, not the scan with its wide uneven borders as it happened to arrive.

**Clicking a citation on a page of a PDF opens the cited case’s own PDF**, in a viewer of its own, so you can follow a chain of authority through the reports as they were printed. The scan is looked for by the routes the **PDF** button already uses: the official U.S. Reports scan whenever a U.S. cite is known – including one recovered through CourtListener’s parallel citations from a Supreme Court Reporter or nominative cite – the Harvard static.case.law scan for everything else, supremecourt.gov’s slip‑opinion archive for a decision too recent to be in any reporter (matched on its docket), and CourtListener’s own stored copy last of all. A citation with no scan anywhere opens as text, exactly as it always did, and a statute or rule opens in its own viewer as before.

When an opinion is opened from the local database, a **U.S. Reports citation stored with it is the first thing the PDF lookup tries** – wherever it sits in the record’s list of parallel cites – rather than the app going to CourtListener or Google Scholar for a citation it already has.

In every PDF view, zooming in past the width of the window brings up a horizontal scrollbar so the whole page stays reachable: pan with it, with Shift+wheel (or a trackpad’s sideways swipe), or with ← and → after clicking the page. Zooming keeps whatever column is in the middle of the view rather than jumping to the margin, a search result off to one side is panned into view along with being scrolled to, and the bar disappears again as soon as the page fits. **Fit** (or Ctrl/Cmd+0) returns to a page sized to the window – and the page is re-fitted whenever the room it has changes, so it is never left clipped by a window narrower than it.

Quick Lookup (Ctrl+S)
Instant citation lookup: paste a case citation (410 U.S. 113), a statute (42 USC 1983), a regulation (29 CFR 1614.105), or a Federal Rule (FRE 404), and open the source directly.

Open Citation List
Bulk‑open multiple citations – one per line (case names optional).
The app resolves each one via Google Scholar and then CourtListener.

Browse Briefs (Ctrl+B)
Open a PDF, Word, RTF, or text brief – all citations are highlighted and clickable, linking directly to the cited source.

Following a pin cite
A citation with a pinpoint page opens the cited case and jumps to that page. A citation to a footnote – “200 U.S. 12, 13 n.4”, “13 n. 4”, “13 nn.4–5”, “13 & n.4” – is read as a whole: the note number is part of the link, and following it lands on note 4 rather than on the page its reference sits on. Where a report carries several writings that each begin their notes at 1, the page in the pin cite is what says whose note 4 is meant. Short forms (“542 U. S., at 254 n.9”) and “Id., at 23 n.2” work the same way.

What Sources Are Included
Source	Description
CourtListener	Full‑text search across U.S. federal and state court opinions. Provides PDFs and structured opinion text.
Google Scholar	Opinion text with formatting, citations, and separate opinions (majority, concurrence, dissent). Used as primary text viewer.
U.S. Code	Current law from the Office of the Law Revision Counsel (OLRC). Renders with indentation and enumerator hierarchy.
Code of Federal Regulations	eCFR API – current regulations, section‑by‑section.
Federal Rules	Civil, Criminal, Evidence, Appellate, Bankruptcy – from Cornell LII.
U.S. Constitution	Full text with article/amendment navigation; detects both formal citations and prose references.
California & Florida Statutes	Official texts from the state legislatures (CA LegInfo, FL Senate). More states can be added.
Statutes at Large	U.S. Statutes at Large (GovInfo PDFs) – cited as 88 Stat. 1932.
US Reports PDFs	Official Supreme Court opinion scans: GPO’s GovInfo (vols 2–583) first, the Library of Congress CDN (vols 1–542) as fallback; for vols 584+ the app downloads the Court’s own bound‑volume / preliminary‑print PDF from supremecourt.gov into the “US Reports” folder (once per volume) and carves the cited opinion out of it. If those sources have no PDF for a post-2020 decision, the app matches its docket, citation, or caption and date against the Court’s slip-opinion archive, with CourtListener as the final fallback. A recent opinion reaches Google Scholar with the Supreme Court Reporter’s star pagination, or with none at all, so its U.S. Reports pages are worked out by matching the text against that scan – and the result is then **saved with the opinion in the local database**, so the next time it is opened the pages are there at once and a pin cite goes straight to its page. The scan is still fetched in the background and the saved pages replaced if the alignment has moved on (a preliminary print superseded by the bound volume).
English Reports	Pre‑1865 English case law from CommonLII – offline index + CloudFlare‑aware PDF download (via Firefox cookies or Playwright).
Federal Cases	Pre‑1880 lower federal opinions cited by case number ("Cole v. The Atlantic, Case No. 2,976", chained "Id. 2,717") – no digital number‑to‑reporter index exists, so the case is found live on CourtListener by the printed name (OCR‑forgiving), confirmed by the number at the head of its headnotes or by the F. Cas. volume the number's alphabetical position dictates.
Oyez	Supreme Court case summaries, question presented, holdings, justice vote splits, and oral argument audio links.
Brief Reader	Extracts text from PDF, Word, RTF, and plain text briefs; highlights every citation and makes them clickable.
Tips
The app caches Google Scholar results and PDF downloads to speed up repeated lookups.

For English Reports PDFs, if the app can’t fetch them directly, it will open a browser window for you to pass CloudFlare – once cleared, the PDF downloads automatically.

Ctrl+C / Cmd+C copies from a case viewer in whichever of three styles the **Copy** menu has selected: **Copy without citation**, **Copy with citation** (the default – the Bluebook citation, with its pinpoint page, appended below the text), or **Copy as quote**, which wraps the passage in double quotes, demotes the quotation marks inside it a level so the nesting reads correctly, and sets the citation one space after the closing quote the way a brief does. The choice is remembered between sessions. In a case viewer, **Edit citation…** lets you correct the base citation once; the correction is saved locally and reused while pinpoint pages continue to be added automatically.

You can export opinions from the Export ▾ menu: as RTF (two‑column, with running heads) for word processors; as a print‑ready PDF typeset with LaTeX (single column, justified, Century Schoolbook, footnotes at the foot of the page that cites them, and a running head showing the reporter page range visible on each sheet) if a LaTeX installation (TeX Live, MiKTeX, or Tectonic) is available; or as **.tex source** – the same document the PDF export typesets, saved unbuilt so you can edit it first. Both exports break the opinion into sections the way a reporter prints it: front matter the report names – a **Syllabus**, the reporter's **Headnotes**, or, in the early volumes, the **Argument of Counsel** – leads on pages of its own headed by that name, each separate opinion follows on a fresh page headed by its author, and an ordinary caption (docket number, dates, counsel listings) stays on the opinion's first page where it belongs. The .tex export needs no LaTeX installed here, and is offered automatically if you ask for the PDF on a machine without an engine.

Troubleshooting
“Missing Token” – answer Yes to enter your CourtListener token, or paste it later via Settings → API Token….

Google Scholar not working – install beautifulsoup4 (pip install beautifulsoup4).

PDF viewer not working – install pypdfium2 and Pillow (pip install pypdfium2 Pillow).

English Reports CloudFlare issues – ensure you have curl_cffi, browser_cookie3, and Playwright installed, and run playwright install chromium. Firefox users can also clear the check in Firefox once – the app will reuse that cookie.

License & Credits
This tool is built on top of the excellent free legal data sources:

CourtListener – Free Law Project

Google Scholar

Oyez – Cornell LII / Chicago‑Kent

eCFR – GPO / OFR

OLRC – U.S. Code

Cornell LII – Federal Rules

CommonLII – English Reports

All content remains the property of its respective owners.
