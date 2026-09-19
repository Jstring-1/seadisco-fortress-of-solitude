// ── AI-music channel detection ────────────────────────────────────
//
// Pure scoring for the admin "AI channel hunt" (YT review tab): given a
// batch of YouTube videos (search results or review-queue candidates) and
// optional channel profiles, score each channel on signs that it posts
// AI-generated music. Nothing is banned automatically — channels at or
// above AI_FLAG_THRESHOLD become candidates for the admin to review.
//
// Signals, and where they count most (a label is scored once per
// channel, at its strongest location):
//   YouTube "altered or synthetic content" disclosure on a video   +5
//   AI wording / tool name in the channel name                      +4
//   … in a video title                                              +3
//   … in a video or channel description                             +2
//   the bare word "AI" in the channel name / a title                +2 / +1
//   young channel with a very high upload count                     +2

export const AI_FLAG_THRESHOLD = 3;

export interface AiVideo {
  videoId: string;
  title: string;
  description: string;
  channelId: string;
  channelTitle: string;
  publishedAt?: string;
  thumbnail?: string;
  synthetic?: boolean | null;   // status.containsSyntheticMedia, when fetched
}
export interface AiChannelProfile {
  title?: string;
  description?: string;
  publishedAt?: string;
  videoCount?: number | null;
  gone?: boolean;
}
export interface AiSample { videoId: string; title: string; thumbnail: string; publishedAt: string }
export interface AiChannelFinding {
  channelId: string;
  channelTitle: string;
  score: number;
  signals: string[];
  samples: AiSample[];
}

const STRONG: Array<[RegExp, string]> = [
  [/\bsuno\b/i, "mentions Suno"],
  [/\budio\b/i, "mentions Udio"],
  [/\b(riffusion|mubert|boomy|soundraw|aiva|musicgen|stable audio|beatoven|soundful)\b/i, "names an AI music tool"],
  [/\bai[\s-]?(generated|music|song|songs|cover|covers|voice|vocals|singer|band|artist|album|blues|jazz|country|gospel|soul|folk|rock|made|created|composed|remake)\b/i, "“AI …” wording"],
  [/\b(generated|made|created|produced|composed|written|sung) (with|by|using|via|through) (an? )?(ai|a\.i\.|artificial intelligence)\b/i, "says it was made with AI"],
  [/\bartificial intelligence\b/i, "mentions artificial intelligence"],
  [/#ai(music|generated|cover|song|songs|art|artist|blues|jazz|singer)?\b/i, "#AI hashtag"],
  [/\b(not|isn't|is not) a real (artist|band|singer|person|musician)\b/i, "says the artist isn't real"],
  [/\b(fictional|virtual|imaginary) (artist|band|singer|bluesman|musician|jazzman)\b/i, "fictional / virtual artist"],
];
// Case-sensitive: "AI" as a standalone word (lowercase "ai" is common in
// other languages and names).
const WEAK_AI = /(^|[^A-Za-z])A\.?I\.?(?![A-Za-z])/;

export function aiStrongLabels(text: string): string[] {
  const t = String(text || "");
  return STRONG.filter(([re]) => re.test(t)).map(([, label]) => label);
}

function channelAgeYears(publishedAt?: string): number | null {
  const ms = publishedAt ? Date.parse(publishedAt) : NaN;
  return Number.isFinite(ms) ? (Date.now() - ms) / (365.25 * 86400000) : null;
}

export function scoreAiChannels(
  videos: AiVideo[],
  profiles: Map<string, AiChannelProfile> = new Map(),
): AiChannelFinding[] {
  const byChannel = new Map<string, AiVideo[]>();
  for (const v of videos) {
    if (!v?.channelId) continue;
    if (!byChannel.has(v.channelId)) byChannel.set(v.channelId, []);
    byChannel.get(v.channelId)!.push(v);
  }
  const out: AiChannelFinding[] = [];
  for (const [channelId, vids] of byChannel) {
    const prof = profiles.get(channelId);
    if (prof?.gone) continue;
    const channelTitle = String(prof?.title || vids[0].channelTitle || "");
    // Strongest location per label.
    const best = new Map<string, number>();
    const bump = (label: string, pts: number) => { if ((best.get(label) ?? 0) < pts) best.set(label, pts); };
    for (const l of aiStrongLabels(channelTitle)) bump(l, 4);
    for (const v of vids) {
      for (const l of aiStrongLabels(v.title)) bump(l, 3);
      for (const l of aiStrongLabels(v.description)) bump(l, 2);
    }
    for (const l of aiStrongLabels(prof?.description ?? "")) bump(l, 2);
    if (vids.some(v => v.synthetic === true)) bump("YouTube “altered or synthetic” label", 5);
    // The bare word only counts where no stronger AI wording already matched.
    const weak = (t: string) => WEAK_AI.test(t) && aiStrongLabels(t).length === 0;
    if (weak(channelTitle)) bump("“AI” in the channel name", 2);
    else if (vids.some(v => weak(v.title))) bump("“AI” in a title", 1);
    const age = channelAgeYears(prof?.publishedAt);
    const count = Number(prof?.videoCount ?? 0);
    if (age != null && ((count >= 300 && age <= 2) || (count >= 1000 && age <= 3))) {
      const since = new Date(Date.parse(prof!.publishedAt!)).toLocaleDateString("en-US", { month: "short", year: "numeric" });
      bump(`${count.toLocaleString("en-US")} videos since ${since}`, 2);
    }
    const score = [...best.values()].reduce((a, b) => a + b, 0);
    if (score <= 0) continue;
    const signals = [...best.entries()].sort((a, b) => b[1] - a[1]).map(([l]) => l);
    // Samples: videos that show a signal first.
    const flagged = (v: AiVideo) => v.synthetic === true || aiStrongLabels(`${v.title} ${v.description}`).length > 0 || WEAK_AI.test(v.title);
    const samples = [...vids].sort((a, b) => Number(flagged(b)) - Number(flagged(a))).slice(0, 6).map(v => ({
      videoId: v.videoId, title: v.title, thumbnail: v.thumbnail ?? "", publishedAt: v.publishedAt ?? "",
    }));
    out.push({ channelId, channelTitle, score, signals, samples });
  }
  return out.sort((a, b) => b.score - a.score);
}
