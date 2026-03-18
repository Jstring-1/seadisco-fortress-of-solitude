// One-time seed script: populates search_history with a diverse set of searches
// Usage: APP_DB_URL="postgresql://..." node scripts/seed-searches.js
import pg from "pg";

const { Pool } = pg;
const pool = new Pool({ connectionString: process.env.APP_DB_URL, ssl: { rejectUnauthorized: false } });

// Spread timestamps over the past 90 days so they look organic
function randomDate() {
  const now = Date.now();
  const ninetyDays = 90 * 24 * 60 * 60 * 1000;
  return new Date(now - Math.random() * ninetyDays).toISOString();
}

const seeds = [
  // ── Post-war Blues ───────────────────────────────────────────────────────
  { artist: "Muddy Waters" },
  { artist: "Howlin' Wolf" },
  { artist: "Little Walter" },
  { artist: "Sonny Boy Williamson" },
  { artist: "B.B. King" },
  { artist: "Albert King" },
  { artist: "Freddie King" },
  { artist: "John Lee Hooker" },
  { artist: "Lightnin' Hopkins" },
  { artist: "T-Bone Walker" },
  { artist: "Jimmy Reed" },
  { artist: "Elmore James" },
  { artist: "Buddy Guy" },
  { artist: "Junior Wells" },
  { artist: "Magic Sam" },
  { artist: "Otis Rush" },
  { artist: "Bobby Blue Bland" },
  { artist: "Little Milton" },
  { artist: "James Cotton" },
  { artist: "Ike Turner" },
  { artist: "Slim Harpo" },
  { artist: "Lazy Lester" },
  { artist: "Lonesome Sundown" },
  { artist: "Clarence "Gatemouth" Brown" },
  { artist: "Guitar Slim" },
  { artist: "Earl Hooker" },
  { artist: "Snooky Pryor" },
  { artist: "Robert Nighthawk" },
  { q: "Chicago blues", genre: "Blues", style: "Chicago Blues" },
  { q: "electric blues", genre: "Blues", style: "Electric Blues" },
  { q: "Texas blues", genre: "Blues", style: "Texas Blues" },
  { label: "Chess" },
  { label: "Excello" },
  { label: "Cobra" },

  // ── 1950s/60s Jazz ──────────────────────────────────────────────────────
  { artist: "Miles Davis" },
  { artist: "John Coltrane" },
  { artist: "Thelonious Monk" },
  { artist: "Charles Mingus" },
  { artist: "Sonny Rollins" },
  { artist: "Bill Evans" },
  { artist: "Dave Brubeck" },
  { artist: "Art Blakey" },
  { artist: "Clifford Brown" },
  { artist: "Lee Morgan" },
  { artist: "Wes Montgomery" },
  { artist: "McCoy Tyner" },
  { artist: "Herbie Hancock" },
  { artist: "Wayne Shorter" },
  { artist: "Hank Mobley" },
  { artist: "Dexter Gordon" },
  { artist: "Coleman Hawkins" },
  { artist: "Lester Young" },
  { artist: "Cannonball Adderley" },
  { artist: "Donald Byrd" },
  { artist: "Kenny Dorham" },
  { artist: "Freddie Hubbard" },
  { artist: "Grant Green" },
  { artist: "Jimmy Smith" },
  { artist: "Horace Silver" },
  { artist: "Art Farmer" },
  { artist: "Booker Little" },
  { artist: "Jackie McLean" },
  { artist: "Andrew Hill" },
  { artist: "Eric Dolphy" },
  { q: "hard bop", genre: "Jazz", style: "Hard Bop" },
  { q: "cool jazz", genre: "Jazz", style: "Cool Jazz" },
  { q: "modal jazz", genre: "Jazz", style: "Modal" },
  { q: "post bop", genre: "Jazz", style: "Post Bop" },
  { label: "Blue Note" },
  { label: "Prestige" },
  { label: "Riverside" },
  { label: "Impulse!" },
  { label: "Verve" },

  // ── Mbalax / Semba / West & Central African ─────────────────────────────
  { artist: "Orchestra Baobab" },
  { artist: "Bembeya Jazz National" },
  { artist: "Balla et ses Balladins" },
  { artist: "Laba Sosseh" },
  { artist: "Super Mama Djombo" },
  { artist: "Franco" },
  { artist: "Tabu Ley Rochereau" },
  { artist: "Sam Mangwana" },
  { artist: "Orchestre Poly-Rythmo de Cotonou" },
  { artist: "T.P. Orchestre Poly-Rythmo" },
  { artist: "Bonga" },
  { artist: "Carlos Burity" },
  { artist: "Eduardo Nascimento" },
  { artist: "Miriam Makeba" },
  { artist: "Hugh Masekela" },
  { artist: "Youssou N'Dour" },
  { artist: "Etoile de Dakar" },
  { artist: "Rail Band" },
  { artist: "Les Ambassadeurs" },
  { artist: "Star Band de Dakar" },
  { q: "African rumba Congo", genre: "Folk, World, & Country" },
  { q: "semba Angola", genre: "Folk, World, & Country" },
  { q: "mbalax Senegal", genre: "Folk, World, & Country" },
  { q: "Guinea 1960s", genre: "Folk, World, & Country" },

  // ── Early Appalachian / Old Time ────────────────────────────────────────
  { artist: "Dock Boggs" },
  { artist: "Clarence Ashley" },
  { artist: "Roscoe Holcomb" },
  { artist: "Doc Watson" },
  { artist: "Carter Family" },
  { artist: "Charlie Poole" },
  { artist: "Gid Tanner" },
  { artist: "Ernest Stoneman" },
  { artist: "Buell Kazee" },
  { artist: "Jean Ritchie" },
  { artist: "Almeda Riddle" },
  { artist: "New Lost City Ramblers" },
  { artist: "Ralph Stanley" },
  { artist: "Hobart Smith" },
  { artist: "Nimrod Workman" },
  { artist: "Kilby Snow" },
  { artist: "Wade Ward" },
  { artist: "Clint Howard" },
  { q: "Appalachian music old time", genre: "Folk, World, & Country", style: "Appalachian Music" },
  { q: "old time string band", genre: "Folk, World, & Country" },
  { q: "mountain music banjo", genre: "Folk, World, & Country" },
  { label: "Folkways" },
  { label: "Rounder" },

  // ── 50s/60s/70s World Music ─────────────────────────────────────────────
  { artist: "Oum Kalthoum" },
  { artist: "Fairuz" },
  { artist: "Abdel Halim Hafez" },
  { artist: "Mohammed Abdel Wahab" },
  { artist: "Warda" },
  { artist: "Ravi Shankar" },
  { artist: "Ali Akbar Khan" },
  { artist: "Bismillah Khan" },
  { artist: "Vilayat Khan" },
  { artist: "Mulatu Astatke" },
  { artist: "Alemayehu Eshete" },
  { artist: "Mahmoud Ahmed" },
  { artist: "Ali Hassan Kuban" },
  { artist: "Stella Chiweshe" },
  { artist: "Thomas Mapfumo" },
  { artist: "Los Indios Tabajaras" },
  { artist: "Trio Los Panchos" },
  { artist: "Agustin Lara" },
  { artist: "Pedro Infante" },
  { artist: "Jorge Ben" },
  { artist: "Joao Gilberto" },
  { artist: "Caetano Veloso" },
  { artist: "Gilberto Gil" },
  { artist: "Maria Bethania" },
  { artist: "Chico Buarque" },
  { artist: "Nana Mouskouri" },
  { artist: "Mikis Theodorakis" },
  { artist: "Stelios Kazantzidis" },
  { artist: "Ivo Lola Ribar" },
  { artist: "Mahotella Queens" },
  { q: "Ethiopian jazz Ethio jazz", genre: "Folk, World, & Country" },
  { q: "Egyptian classical Arabic", genre: "Folk, World, & Country" },
  { q: "Indian classical sitar", genre: "Folk, World, & Country" },
  { q: "Bossa Nova 1960s", genre: "Latin", style: "Bossa Nova" },
  { q: "Tropicalia Brazil", genre: "Folk, World, & Country" },
  { label: "Philips" },
  { label: "Nonesuch" },
];

async function main() {
  let inserted = 0;
  for (const params of seeds) {
    const ts = randomDate();
    await pool.query(
      `INSERT INTO search_history (clerk_user_id, params, searched_at) VALUES ($1, $2, $3)
       ON CONFLICT DO NOTHING`,
      ["system-seed", JSON.stringify(params), ts]
    );
    inserted++;
  }
  console.log(`Inserted ${inserted} seed searches.`);
  await pool.end();
}

main().catch(e => { console.error(e); process.exit(1); });
