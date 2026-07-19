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

Launch the app – the first time you search, it will ask for the token.
You can also enter it later via Settings → API Token… in the menu bar.

The token is stored locally (~/.config/courtlistener/config.json) and is used for all CourtListener API requests.

3. Run the application
bash
python courtlistener_gui.py
On first start, the main window will be hidden – it runs in the background.

Press Ctrl+Space (or Cmd+Space on macOS) to open a quick‑search popup.

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

Click Google Scholar Text to fetch the full opinion from Google Scholar (often richer than CourtListener’s HTML).

Quick Lookup (Ctrl+S)
Instant citation lookup: paste a case citation (410 U.S. 113), a statute (42 USC 1983), a regulation (29 CFR 1614.105), or a Federal Rule (FRE 404), and open the source directly.

Open Citation List
Bulk‑open multiple citations – one per line (case names optional).
The app resolves each one via Google Scholar and then CourtListener.

Browse Briefs (Ctrl+B)
Open a PDF, Word, RTF, or text brief – all citations are highlighted and clickable, linking directly to the cited source.

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
US Reports PDFs	Official Supreme Court opinion scans: GPO’s GovInfo (vols 2–583) first, the Library of Congress CDN (vols 1–542) as fallback; for vols 584+ the app downloads the Court’s own bound‑volume / preliminary‑print PDF from supremecourt.gov into the “US Reports” folder (once per volume) and carves the cited opinion out of it.
English Reports	Pre‑1865 English case law from CommonLII – offline index + CloudFlare‑aware PDF download (via Firefox cookies or Playwright).
Federal Cases	Pre‑1880 lower federal opinions cited by case number ("Cole v. The Atlantic, Case No. 2,976", chained "Id. 2,717") – no digital number‑to‑reporter index exists, so the case is found live on CourtListener by the printed name (OCR‑forgiving), confirmed by the number at the head of its headnotes or by the F. Cas. volume the number's alphabetical position dictates.
Oyez	Supreme Court case summaries, question presented, holdings, justice vote splits, and oral argument audio links.
Brief Reader	Extracts text from PDF, Word, RTF, and plain text briefs; highlights every citation and makes them clickable.
Tips
The app caches Google Scholar results and PDF downloads to speed up repeated lookups.

For English Reports PDFs, if the app can’t fetch them directly, it will open a browser window for you to pass CloudFlare – once cleared, the PDF downloads automatically.

All text viewers support copy with Bluebook citation (Ctrl+C / Cmd+C) – the copied text includes a properly formatted citation appended at the end. In a case viewer, **Edit citation…** lets you correct the base citation once; the correction is saved locally and reused while pinpoint pages continue to be added automatically.

You can export opinions from the Export ▾ menu: as RTF (two‑column, with running heads) for word processors; as a print‑ready PDF typeset with LaTeX (single column, justified, Century Schoolbook, footnotes at the foot of the page that cites them, and a running head showing the reporter page range visible on each sheet) if a LaTeX installation (TeX Live, MiKTeX, or Tectonic) is available; or as Markdown with footnotes and star pagination preserved – offered automatically when LaTeX isn’t installed.

Troubleshooting
“Missing Token” – go to Settings → API Token… and paste your CourtListener token.

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
