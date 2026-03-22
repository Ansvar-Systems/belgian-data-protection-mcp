/**
 * Seed the GBA/APD database with sample decisions and guidelines for testing.
 *
 * Includes real GBA/APD decisions (Google Belgium, IAB TCF, RTB)
 * and representative guidance documents so MCP tools can be tested without
 * running a full data ingestion pipeline.
 *
 * Usage:
 *   npx tsx scripts/seed-sample.ts
 *   npx tsx scripts/seed-sample.ts --force   # drop and recreate
 */

import Database from "better-sqlite3";
import { existsSync, mkdirSync, unlinkSync } from "node:fs";
import { dirname } from "node:path";
import { SCHEMA_SQL } from "../src/db.js";

const DB_PATH = process.env["GBA_DB_PATH"] ?? "data/gba.db";
const force = process.argv.includes("--force");

// --- Bootstrap database ------------------------------------------------------

const dir = dirname(DB_PATH);
if (!existsSync(dir)) {
  mkdirSync(dir, { recursive: true });
}

if (force && existsSync(DB_PATH)) {
  unlinkSync(DB_PATH);
  console.log(`Deleted existing database at ${DB_PATH}`);
}

const db = new Database(DB_PATH);
db.pragma("journal_mode = WAL");
db.pragma("foreign_keys = ON");
db.exec(SCHEMA_SQL);

console.log(`Database initialised at ${DB_PATH}`);

// --- Topics ------------------------------------------------------------------

interface TopicRow {
  id: string;
  name_nl: string;
  name_en: string;
  description: string;
}

const topics: TopicRow[] = [
  {
    id: "cookies",
    name_nl: "Cookies en trackers",
    name_en: "Cookies and trackers",
    description: "Plaatsen en lezen van cookies en trackers op terminals van gebruikers (ePrivacy-richtlijn, art. 129 Wet elektronische communicatie).",
  },
  {
    id: "direct_marketing",
    name_nl: "Direct marketing",
    name_en: "Direct marketing",
    description: "Verwerking van persoonsgegevens voor direct-marketingdoeleinden (art. 6(1)(f) en Overweging 47 AVG).",
  },
  {
    id: "camerabewaking",
    name_nl: "Camerabewaking",
    name_en: "CCTV surveillance",
    description: "Camerabewaking van publieke en private ruimten (Wet camerabewaking van 21 maart 2007).",
  },
  {
    id: "werknemerscontrole",
    name_nl: "Werknemerscontrole",
    name_en: "Employee monitoring",
    description: "Controle van werknemers door werkgevers, inclusief e-mailmonitoring en locatiebepaling.",
  },
  {
    id: "doorgiften",
    name_nl: "Internationale doorgiften",
    name_en: "International transfers",
    description: "Doorgifte van persoonsgegevens naar derde landen (art. 44–49 AVG).",
  },
  {
    id: "toestemming",
    name_nl: "Toestemming",
    name_en: "Consent",
    description: "Geldige toestemming als rechtsgrondslag voor verwerking (art. 7 AVG).",
  },
  {
    id: "gegevensbescherming_effect_beoordeling",
    name_nl: "Gegevensbeschermingseffectbeoordeling",
    name_en: "Data Protection Impact Assessment",
    description: "Beoordeling van gegevensbeschermingsrisico bij risicovolle verwerkingen (art. 35 AVG).",
  },
  {
    id: "kinderen",
    name_nl: "Gegevens van kinderen",
    name_en: "Children's data",
    description: "Verwerking van persoonsgegevens van minderjarigen (art. 8 AVG).",
  },
  {
    id: "profilering",
    name_nl: "Profilering",
    name_en: "Profiling",
    description: "Geautomatiseerde verwerking ter beoordeling van persoonlijke aspecten (art. 22 AVG).",
  },
];

const insertTopic = db.prepare(
  "INSERT OR IGNORE INTO topics (id, name_nl, name_en, description) VALUES (?, ?, ?, ?)",
);

for (const t of topics) {
  insertTopic.run(t.id, t.name_nl, t.name_en, t.description);
}

console.log(`Inserted ${topics.length} topics`);

// --- Decisions ---------------------------------------------------------------

interface DecisionRow {
  reference: string;
  title: string;
  date: string;
  type: string;
  entity_name: string;
  fine_amount: number | null;
  summary: string;
  full_text: string;
  topics: string;
  gdpr_articles: string;
  status: string;
}

const decisions: DecisionRow[] = [
  // Google Belgium — EUR 600K fine
  {
    reference: "GBA-2022-DOS-2019-04710",
    title: "Beslissing ten gronde 01/2022 — Google LLC",
    date: "2022-02-14",
    type: "sanction",
    entity_name: "Google LLC",
    fine_amount: 600_000,
    summary:
      "De GBA heeft Google LLC beboet met 600.000 euro wegens niet-naleving van het recht op verwijdering ('recht om vergeten te worden') voor zoekresultaten die betrekking hadden op een privépersoon. Google weigerde gedurende meerdere jaren de verwijderingsverzoeken in te willigen. Schending van art. 17 AVG.",
    full_text:
      "De Geschillenkamer van de Gegevensbeschermingsautoriteit heeft in deze zaak vastgesteld dat Google LLC meerdere verwijderingsverzoeken van een betrokken persoon niet correct heeft behandeld. De betrokken persoon had herhaaldelijk verzocht om de verwijdering van zoekresultaten die zijn naam koppelden aan een strafrechtelijk verleden waarvoor hij was vrijgesproken. Google had de verwijderingsverzoeken aanvankelijk geweigerd omdat het meende dat de informatie nog steeds van publiek belang was. De Geschillenkamer stelde vast dat Google het recht op gegevenswissing (art. 17 AVG) had geschonden doordat: (1) de afweging tussen het recht op privacy van de betrokken persoon en het recht van het publiek op informatie niet correct was gemaakt — het publiek belang bij de informatie was niet meer aanwezig nu de betrokken persoon was vrijgesproken; (2) Google niet tijdig had gereageerd op de verwijderingsverzoeken en de betrokken persoon niet adequaat had geïnformeerd over de redenen voor de weigering. De GBA legde een boete op van 600.000 euro en beval Google om de betrokken zoekresultaten binnen 30 dagen te verwijderen.",
    topics: JSON.stringify(["profilering"]),
    gdpr_articles: JSON.stringify(["17", "12"]),
    status: "final",
  },
  // IAB Europe Transparency & Consent Framework
  {
    reference: "GBA-2022-DOS-2019-13994",
    title: "Beslissing ten gronde 21/2022 — IAB Europe (Transparency & Consent Framework)",
    date: "2022-02-02",
    type: "sanction",
    entity_name: "IAB Europe",
    fine_amount: 250_000,
    summary:
      "De GBA sanctioneerde IAB Europe met 250.000 euro wegens het Transparency & Consent Framework (TCF), dat wordt gebruikt voor real-time bidding in de online reclamesector. Het TCF voldoet niet aan de AVG-vereisten voor geldige toestemming: de toestemming is niet vrij, specifiek en ondubbelzinnig gegeven.",
    full_text:
      "De Geschillenkamer van de Gegevensbeschermingsautoriteit heeft vastgesteld dat het Transparency & Consent Framework (TCF) van IAB Europe, het systeem dat door de online advertentiesector wordt gebruikt om toestemming van gebruikers te registreren en door te geven, niet in overeenstemming is met de Algemene Verordening Gegevensbescherming (AVG). De voornaamste vaststellingen zijn: (1) IAB Europe is verwerkingsverantwoordelijke voor de TC String — de gecodeerde string die de toestemmingsvoorkeuren van gebruikers bevat; de Geschillenkamer verwierp het standpunt van IAB Europe dat het slechts een brancheorganisatie was zonder verwerkingsverantwoordelijkheid; (2) de TC String bevat persoonsgegevens — de combinatie van toestemmingsvoorkeuren met de IP-adressen van gebruikers maakt hen identificeerbaar; (3) de toestemming via het TCF voldoet niet aan de AVG-vereisten: de 'cookie walls' waarbij toegang tot content afhankelijk wordt gemaakt van toestemming voor advertentiecookies maken de toestemming niet vrij; de cookiebanner maakt het moeilijker om te weigeren dan om te aanvaarden; (4) deelnemers aan het TCF (publishers, SSPs, DSPs) verwerken op basis van 'legitimate interest' gegevens voor doeleinden waarvoor zij geen geldige rechtsgrondslag hebben. De GBA beval IAB Europe om het TCF in overeenstemming te brengen met de AVG binnen een termijn van 6 maanden.",
    topics: JSON.stringify(["cookies", "toestemming", "profilering"]),
    gdpr_articles: JSON.stringify(["4", "6", "7", "24"]),
    status: "final",
  },
  // Real-time bidding
  {
    reference: "GBA-2021-DOS-2019-01377",
    title: "Beslissing ten gronde 35/2021 — Real-time bidding (RTB)",
    date: "2021-05-19",
    type: "beslissing",
    entity_name: "Diverse RTB-actoren",
    fine_amount: null,
    summary:
      "De GBA stelde in een onderzoeksrapport vast dat het real-time bidding ecosysteem in de online advertentiesector structureel in strijd is met de AVG. Het systeem verspreidt gedetailleerde persoonsgegevens van miljoenen gebruikers naar honderden bedrijven zonder geldige rechtsgrondslag.",
    full_text:
      "De Inspectiedienst van de Gegevensbeschermingsautoriteit heeft een grondig onderzoek uitgevoerd naar het ecosysteem van real-time bidding (RTB) in de online advertentiesector. RTB is een geautomatiseerd veilingsysteem waarbij advertentieplekken in real-time worden geveild aan de hoogste bieder. Hierbij worden gedetailleerde persoonsgegevens van websitebezoekers in milliseconden naar honderden advertentiebedrijven gestuurd. De Inspectiedienst stelde de volgende fundamentele problemen vast: (1) Ontbreken van een geldige rechtsgrondslag — de meeste RTB-actoren verwerken persoonsgegevens op basis van toestemming (via cookie-banners) of gerechtvaardigde belangen; de toestemming via cookiebanners voldoet echter niet aan de AVG-vereisten (niet vrij, niet geïnformeerd, niet ondubbelzinnig); (2) Gebrek aan transparantie — gebruikers weten niet welke gegevens over hen worden verzameld en met hoeveel partijen deze worden gedeeld; (3) Ongecontroleerde verspreiding van persoonsgegevens — de 'bid request' die bij elke advertentieveiling wordt verstuurd, bevat gedetailleerde gegevens (browser, apparaat, locatie, surfgedrag, vermoedelijke interesses); (4) Grensoverschrijdende doorgiften — veel RTB-actoren zijn gevestigd buiten de EER, waarvoor geen adequate garanties bestaan.",
    topics: JSON.stringify(["cookies", "toestemming", "doorgiften", "profilering"]),
    gdpr_articles: JSON.stringify(["5", "6", "7", "13", "44"]),
    status: "final",
  },
  // Direct marketing — TDG
  {
    reference: "GBA-2020-DOS-2019-06654",
    title: "Beslissing ten gronde 38/2020 — Telefoongids (direct marketing)",
    date: "2020-06-15",
    type: "sanction",
    entity_name: "Belgische Telefoongids NV",
    fine_amount: 20_000,
    summary:
      "De GBA beboette de Belgische Telefoongids voor het verwerken van telefoonnummers van klanten voor direct-marketingdoeleinden zonder geldige toestemming. Klanten hadden eerder bezwaar gemaakt tegen direct marketing maar bleven reclameboodschappen ontvangen.",
    full_text:
      "De Geschillenkamer van de Gegevensbeschermingsautoriteit heeft vastgesteld dat de Belgische Telefoongids NV het bezwaarrecht (art. 21 AVG) van klanten niet heeft gerespecteerd. Meerdere klanten hadden via de wettelijk voorziene procedure bezwaar gemaakt tegen de verwerking van hun persoonsgegevens voor direct-marketingdoeleinden. Ondanks deze bezwaren bleven de betrokkenen reclameboodschappen ontvangen. De GBA stelde de volgende schendingen vast: (1) Schending van art. 21 AVG — het bezwaar tegen verwerking voor direct-marketingdoeleinden moet absoluut worden gerespecteerd; de verwerkingsverantwoordelijke mag de gegevens dan niet langer voor dat doel verwerken; (2) Onvoldoende organisatorische maatregelen — de interne processen van de Belgische Telefoongids waren onvoldoende om te waarborgen dat bezwaren tijdig en volledig werden verwerkt in alle systemen; (3) Schending van de informatieplicht — de communicatie over hoe en wanneer bezwaar kan worden gemaakt was niet duidelijk genoeg.",
    topics: JSON.stringify(["direct_marketing", "toestemming"]),
    gdpr_articles: JSON.stringify(["6", "21"]),
    status: "final",
  },
];

const insertDecision = db.prepare(`
  INSERT OR IGNORE INTO decisions
    (reference, title, date, type, entity_name, fine_amount, summary, full_text, topics, gdpr_articles, status)
  VALUES
    (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
`);

const insertDecisionsAll = db.transaction(() => {
  for (const d of decisions) {
    insertDecision.run(
      d.reference,
      d.title,
      d.date,
      d.type,
      d.entity_name,
      d.fine_amount,
      d.summary,
      d.full_text,
      d.topics,
      d.gdpr_articles,
      d.status,
    );
  }
});

insertDecisionsAll();
console.log(`Inserted ${decisions.length} decisions`);

// --- Guidelines --------------------------------------------------------------

interface GuidelineRow {
  reference: string | null;
  title: string;
  date: string;
  type: string;
  summary: string;
  full_text: string;
  topics: string;
  language: string;
}

const guidelines: GuidelineRow[] = [
  {
    reference: "GBA-AANBEVELING-COOKIES-2020",
    title: "Aanbeveling 01/2020 — Cookies en andere traceringsmechanismen",
    date: "2020-05-20",
    type: "aanbeveling",
    summary:
      "Aanbeveling van de GBA over de vereisten voor geldige toestemming bij cookies en andere traceringsmechanismen. Behandelt cookie walls, weigeren moet even gemakkelijk zijn als aanvaarden, en de geldigheid van toestemming gegeven via een cookiebanner.",
    full_text:
      "De Gegevensbeschermingsautoriteit heeft een aanbeveling gepubliceerd over de vereisten voor cookies en andere traceringsmechanismen. Deze aanbeveling verduidelijkt de vereisten die voortvloeien uit artikel 129 van de wet van 13 juni 2005 betreffende de elektronische communicatie en de AVG. Voornaamste principes: (1) Toestemming vóór plaatsing — cookies die niet strikt noodzakelijk zijn voor de werking van de website mogen pas worden geplaatst nadat de gebruiker geldige toestemming heeft gegeven; (2) Geen cookie walls — het blokkeren van toegang tot content tenzij de gebruiker akkoord gaat met alle cookies maakt de toestemming niet vrij en is dus ongeldig; (3) Weigeren even gemakkelijk als aanvaarden — de gebruikersinterface mag het aanvaarden van cookies niet makkelijker maken dan het weigeren; (4) Gelaagde informatie — de cookiebanner moet duidelijke en beknopte informatie bieden, met de mogelijkheid voor meer gedetailleerde informatie; (5) Bewijslast — de verwerkingsverantwoordelijke moet kunnen aantonen dat geldige toestemming is verkregen; (6) Duur van de toestemming — de toestemming voor cookies heeft een maximale geldigheidsduur van 13 maanden.",
    topics: JSON.stringify(["cookies", "toestemming"]),
    language: "nl",
  },
  {
    reference: "GBA-AANBEVELING-CAMERA-2020",
    title: "Aanbeveling 01/2020 betreffende camerabewaking (Wet van 21 maart 2007)",
    date: "2020-07-15",
    type: "aanbeveling",
    summary:
      "Aanbeveling van de GBA over camerabewaking in publieke en private ruimten. Behandelt de vereisten van de Camerawet, meldingsplicht bij de GBA, informatieplicht via pictogrammen, bewaartermijnen en toegangsrechten.",
    full_text:
      "Camerabewaking in België is geregeld door de wet van 21 maart 2007 tot regeling van de plaatsing en het gebruik van bewakingscamera's (de 'Camerawet') en de AVG. Deze aanbeveling van de GBA verduidelijkt de cumulatieve toepassingsvoorwaarden. Meldingsplicht: Elke bewakingscamera die niet-besloten plaatsen bewaakt, moet voorafgaand aan de plaatsing worden gemeld bij de GBA via de Aangiftewebsite. Voor besloten plaatsen is dit niet verplicht. Informatieplicht: De aanwezigheid van bewakingscamera's moet worden aangegeven via een pictogram dat duidelijk zichtbaar is vóór de bewaakte zone. Het pictogram moet de identiteit van de verwerkingsverantwoordelijke vermelden. Bewaartermijnen: Beelden van bewakingscamera's mogen maximaal gedurende één maand worden bewaard, tenzij er een specifieke wettige reden is voor een langere bewaartermijn. Beveiligde zones: In bepaalde beveiligde zones (bewapende transporten, juweliers, nachtwinkels) gelden verlengde bewaartermijnen. Toegang tot beelden: De toegang tot camerabeelden is beperkt tot personen die specifiek door de verwerkingsverantwoordelijke zijn gemachtigd.",
    topics: JSON.stringify(["camerabewaking"]),
    language: "nl",
  },
  {
    reference: "GBA-GUIDE-WERKNEMER-2022",
    title: "Gids voor werkgevers — Verwerking van werknemersgegevens",
    date: "2022-09-01",
    type: "guide",
    summary:
      "Praktische gids van de GBA over de verwerking van persoonsgegevens van werknemers. Behandelt rechtsgrondslag voor personeelsadministratie, controle van e-mail en internet, geolocatie en monitoring op afstand.",
    full_text:
      "Deze praktische gids helpt werkgevers bij het naleven van de AVG bij de verwerking van persoonsgegevens van hun werknemers. Rechtsgrondslag voor personeelsgegevens: De meeste verwerkingen van werknemersgegevens zijn gebaseerd op de uitvoering van de arbeidsovereenkomst (art. 6(1)(b) AVG) of op wettelijke verplichtingen (art. 6(1)(c) AVG). De toestemming van de werknemer is als rechtsgrondslag problematisch vanwege het afhankelijkheidsrelatie met de werkgever. Controle van e-mail en internet: De controle van het e-mail- en internetgebruik van werknemers is onderworpen aan strikte beperkingen. Werkgevers mogen de inhoud van privécommunicatie niet lezen, ook niet als dit via bedrijfsapparatuur plaatsvindt. Enkel de metadata (tijdstip, omvang van bestanden) mag in bepaalde omstandigheden worden gecontroleerd. Geolocatie: GPS-tracking van bedrijfsvoertuigen is in principe toegestaan voor bepaalde doeleinden (planning van ritten, veiligheid), maar de locatie buiten werktijden mag niet worden gevolgd. Telewerken: Bij monitoring van medewerkers die op afstand werken gelden dezelfde beperkingen als op kantoor.",
    topics: JSON.stringify(["werknemerscontrole", "toestemming"]),
    language: "nl",
  },
];

const insertGuideline = db.prepare(`
  INSERT INTO guidelines (reference, title, date, type, summary, full_text, topics, language)
  VALUES (?, ?, ?, ?, ?, ?, ?, ?)
`);

const insertGuidelinesAll = db.transaction(() => {
  for (const g of guidelines) {
    insertGuideline.run(
      g.reference,
      g.title,
      g.date,
      g.type,
      g.summary,
      g.full_text,
      g.topics,
      g.language,
    );
  }
});

insertGuidelinesAll();
console.log(`Inserted ${guidelines.length} guidelines`);

// --- Summary -----------------------------------------------------------------

const decisionCount = (
  db.prepare("SELECT count(*) as cnt FROM decisions").get() as { cnt: number }
).cnt;
const guidelineCount = (
  db.prepare("SELECT count(*) as cnt FROM guidelines").get() as { cnt: number }
).cnt;
const topicCount = (
  db.prepare("SELECT count(*) as cnt FROM topics").get() as { cnt: number }
).cnt;
const decisionFtsCount = (
  db.prepare("SELECT count(*) as cnt FROM decisions_fts").get() as { cnt: number }
).cnt;
const guidelineFtsCount = (
  db.prepare("SELECT count(*) as cnt FROM guidelines_fts").get() as { cnt: number }
).cnt;

console.log(`\nDatabase summary:`);
console.log(`  Topics:         ${topicCount}`);
console.log(`  Decisions:      ${decisionCount} (FTS entries: ${decisionFtsCount})`);
console.log(`  Guidelines:     ${guidelineCount} (FTS entries: ${guidelineFtsCount})`);
console.log(`\nDone. Database ready at ${DB_PATH}`);

db.close();
