#!/usr/bin/env node
/**
 * Barilla Monitor — Data Pipeline
 *
 * Collects data from multiple sources and generates data.json
 * for the real-time reputation dashboard.
 *
 * Sources:
 *   1. OpenSearch (AIQ) — Social media posts, engagement, creators
 *   2. Apify — Google News articles about Barilla
 *   3. Eumetra/Turso — Italian consumer survey data
 *   4. Claude API — AI-generated insights from collected data
 *
 * Runs via GitHub Actions every 6 hours.
 */

const fs = require('fs');

// ===== CONFIGURATION =====
const CONFIG = {
  opensearch: {
    url: process.env.OPENSEARCH_URL || 'http://34.249.17.211:4443/euQesdfkoYcHeCHackathonPl',
    index: process.env.OPENSEARCH_INDEX || 'channel_posts',
    user: process.env.OPENSEARCH_USER || '',
    pass: process.env.OPENSEARCH_PASS || '',
  },
  apify: {
    token: process.env.APIFY_API_TOKEN || '',
    googleNewsActorId: 'lexis-solutions/google-news-scraper',
  },
  turso: {
    url: process.env.TURSO_DATABASE_URL || '',
    token: process.env.TURSO_AUTH_TOKEN || '',
  },
  anthropic: {
    apiKey: process.env.ANTHROPIC_API_KEY || '',
  },
  // Brand keywords for filtering
  brand: {
    keywords: ['barilla', 'pasta barilla', 'al bronzo', 'pesto barilla'],
    competitors: ['de cecco', 'rummo', 'garofalo', 'voiello', 'divella'],
  }
};

// ===== HELPERS =====
function log(source, msg) {
  console.log(`[${new Date().toISOString()}] [${source}] ${msg}`);
}

function formatNum(n) {
  if (n >= 1e6) return (n / 1e6).toFixed(1) + 'M';
  if (n >= 1e3) return (n / 1e3).toFixed(1) + 'K';
  return n.toString();
}

// ===== 1. OPENSEARCH — Social Data =====
async function fetchOpenSearchData(days = 7) {
  log('OPENSEARCH', `Fetching social data for last ${days} days...`);
  const { url, index, user, pass } = CONFIG.opensearch;
  if (!user || !pass) { log('OPENSEARCH', 'SKIP: No credentials'); return null; }

  const auth = Buffer.from(`${user}:${pass}`).toString('base64');
  const from = new Date(Date.now() - days * 86400000).toISOString();
  const prevFrom = new Date(Date.now() - days * 2 * 86400000).toISOString();

  async function query(body) {
    const res = await fetch(`${url}/${index}/_search`, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json', 'Authorization': `Basic ${auth}` },
      body: JSON.stringify(body),
    });
    if (!res.ok) throw new Error(`OS ${res.status}: ${(await res.text()).slice(0, 200)}`);
    return res.json();
  }

  // Main aggregations
  const main = await query({
    size: 0,
    track_total_hits: true,
    query: { range: { published_at: { gte: from } } },
    aggs: {
      platforms: { terms: { field: 'channel.type', size: 10 } },
      total_engagement: { sum: { field: 'engagement' } },
      avg_er: { avg: { field: 'engagement_rate' } },
      daily_volume: {
        date_histogram: { field: 'published_at', calendar_interval: 'day' },
        aggs: { engagement: { sum: { field: 'engagement' } }, avg_er: { avg: { field: 'engagement_rate' } } }
      },
      top_hashtags: { terms: { field: 'hashtags', size: 25 } },
      sponsored: { filter: { term: { is_sponsored: true } } },
      top_creators: {
        terms: { field: 'channel.name', size: 15, order: { total_eng: 'desc' } },
        aggs: {
          total_eng: { sum: { field: 'engagement' } },
          platform: { terms: { field: 'channel.type', size: 1 } },
          followers: { max: { field: 'followers' } },
          avg_er: { avg: { field: 'engagement_rate' } }
        }
      },
      hourly_last24h: {
        filter: { range: { published_at: { gte: 'now-24h' } } },
        aggs: {
          hours: {
            date_histogram: { field: 'published_at', calendar_interval: 'hour' },
            aggs: { engagement: { sum: { field: 'engagement' } } }
          },
          count: { value_count: { field: 'published_at' } }
        }
      }
    }
  });

  // Previous period
  const prev = await query({
    size: 0,
    track_total_hits: true,
    query: { range: { published_at: { gte: prevFrom, lte: from } } },
    aggs: {
      total_engagement: { sum: { field: 'engagement' } },
      avg_er: { avg: { field: 'engagement_rate' } },
      platforms: { terms: { field: 'channel.type', size: 10 } }
    }
  });

  // Latest posts
  const latest = await query({
    size: 50,
    sort: [{ published_at: 'desc' }],
    query: { range: { published_at: { gte: from } } },
    _source: ['caption', 'channel.name', 'channel.type', 'channel.id', 'engagement', 'engagement_rate', 'published_at', 'hashtags', 'post_type', 'post_id', 'is_sponsored', 'followers', 'image_url']
  });

  const aggs = main.aggregations;
  const curTotal = main.hits.total.value;
  const prevTotal = prev.hits.total.value;
  const avgER = aggs.avg_er.value || 0;
  const prevAvgER = prev.aggregations.avg_er.value || 0;
  const totalEng = aggs.total_engagement.value || 0;
  const platforms = aggs.platforms.buckets;
  const sponsoredCount = aggs.sponsored.doc_count;
  const organicCount = curTotal - sponsoredCount;
  const earnedPaidRatio = curTotal > 0 ? organicCount / curTotal : 0;
  const volumeChange = prevTotal > 0 ? ((curTotal - prevTotal) / prevTotal * 100) : 0;
  const erChange = prevAvgER > 0 ? ((avgER - prevAvgER) / prevAvgER * 100) : 0;
  const mentions24h = aggs.hourly_last24h.count.value || 0;

  // Platform diversity (entropy)
  const platCounts = platforms.map(p => p.doc_count);
  const platTotal = platCounts.reduce((a, b) => a + b, 0) || 1;
  const entropy = platCounts.reduce((acc, c) => { const p = c / platTotal; return p > 0 ? acc - p * Math.log2(p) : acc; }, 0);
  const maxEntropy = Math.log2(Math.max(platforms.length, 1)) || 1;
  const platformMixScore = Math.round((entropy / maxEntropy) * 100);

  // Dimension scores
  const dimensions = [
    { name: 'Sentiment', score: Math.min(100, Math.round(50 + avgER * 2000)), color: '#00e676' },
    { name: 'Engagement', score: Math.min(100, Math.round(avgER * 5000)), color: '#3ba4ff' },
    { name: 'Volume', score: Math.min(100, Math.round(curTotal / (days * 0.5) * 10)), color: '#1e90ff' },
    { name: 'Heritage', score: 88, color: '#00e676' },
    { name: 'Consumer Fit', score: 75, color: '#00e5ff' },
    { name: 'Platform Mix', score: platformMixScore, color: platformMixScore > 60 ? '#00e676' : '#ffab00' },
    { name: 'Earned vs Paid', score: Math.round(earnedPaidRatio * 100), color: '#00e676' },
    { name: 'Risk Shield', score: 82, color: '#00e676' },
  ];
  const overallScore = Math.round(dimensions.reduce((s, d) => s + d.score, 0) / dimensions.length);

  // Sentiment proxy
  const sentScore = dimensions[0].score;
  const positivePct = Math.round(sentScore * 0.85);
  const negativePct = Math.max(2, Math.round((100 - sentScore) * 0.3));
  const neutralPct = 100 - positivePct - negativePct;

  // Format posts for feed
  const platTypeMap = { instagram: 'ig', tiktok: 'tt', youtube: 'yt', twitter: 'tw', facebook: 'fb' };

  function buildPostUrl(channelType, channelName, channelId, postId, postType) {
    if (!postId) return null;
    switch (channelType) {
      case 'yt': return `https://www.youtube.com/watch?v=${postId}`;
      case 'tt': return `https://www.tiktok.com/@${channelName}/video/${postId}`;
      case 'ig': return postType === 'reel'
        ? `https://www.instagram.com/reel/${postId}/`
        : `https://www.instagram.com/p/${postId}/`;
      case 'tw': return `https://x.com/${channelName}/status/${postId}`;
      case 'fb': return channelId
        ? `https://www.facebook.com/${channelId}/posts/${postId}`
        : `https://www.facebook.com/posts/${postId}`;
      default: return null;
    }
  }

  const posts = latest.hits.hits.map(hit => {
    const s = hit._source;
    const cType = s.channel?.type || '';
    return {
      platform: platTypeMap[cType] || 'news',
      author: s.channel?.name || 'Unknown',
      time: s.published_at,
      text: (s.caption || '').slice(0, 250),
      engagement: s.engagement || 0,
      engagementRate: s.engagement_rate || 0,
      sentiment: (s.engagement_rate || 0) > 0.03 ? 'pos' : (s.engagement_rate || 0) > 0.01 ? 'neu' : 'neg',
      hashtags: s.hashtags || [],
      imageUrl: s.image_url || null,
      url: buildPostUrl(cType, s.channel?.name, s.channel?.id, s.post_id, s.post_type),
    };
  });

  log('OPENSEARCH', `Done: ${curTotal} posts, score ${overallScore}, ${platforms.length} platforms`);

  return {
    overview: {
      totalPosts: curTotal,
      prevTotalPosts: prevTotal,
      volumeChange: +volumeChange.toFixed(1),
      mentions24h,
      totalEngagement: totalEng,
      avgEngagementRate: avgER,
      erChange: +erChange.toFixed(1),
      organicPct: Math.round(earnedPaidRatio * 100),
      sponsoredCount,
    },
    brandHealth: {
      score: overallScore,
      status: overallScore >= 75 ? 'stabile' : overallScore >= 55 ? 'attenzione' : 'critico',
      dimensions,
    },
    sentiment: {
      netScore: positivePct - negativePct,
      positive: positivePct,
      neutral: neutralPct,
      negative: negativePct,
    },
    platforms: platforms.map(p => ({
      name: p.key,
      count: p.doc_count,
      pctOfTotal: +((p.doc_count / curTotal) * 100).toFixed(1),
    })),
    dailyVolume: aggs.daily_volume.buckets.map(b => ({
      date: b.key_as_string,
      count: b.doc_count,
      engagement: b.engagement.value,
      avgEr: b.avg_er.value,
    })),
    hourlyVolume: aggs.hourly_last24h.hours?.buckets?.map(b => ({
      hour: b.key_as_string,
      count: b.doc_count,
      engagement: b.engagement.value,
    })) || [],
    hashtags: aggs.top_hashtags.buckets.map(h => ({ tag: h.key, count: h.doc_count })),
    creators: aggs.top_creators.buckets.map(c => ({
      name: c.key,
      posts: c.doc_count,
      engagement: c.total_eng.value,
      platform: c.platform.buckets[0]?.key || '',
      followers: c.followers.value || 0,
      avgEr: c.avg_er.value || 0,
    })),
    posts,
  };
}

// ===== 2. APIFY — Google News =====
async function fetchGoogleNews() {
  log('APIFY', 'Fetching Google News for Barilla...');
  const { token } = CONFIG.apify;
  if (!token) { log('APIFY', 'SKIP: No API token'); return null; }

  const actors = [
    { id: 'lexis-solutions~google-news-scraper', input: { query: 'Barilla', country: 'IT', language: 'it', maxItems: 30 } },
    { id: 'lhotanova~google-news-scraper', input: { queries: ['Barilla pasta', 'Barilla brand'], language: 'it', maxItems: 30 } },
    { id: 'apify~google-search-scraper', input: { queries: ['Barilla pasta news Italia'], maxPagesPerQuery: 1, languageCode: 'it', countryCode: 'it', resultsPerPage: 20 } },
  ];

  for (const actor of actors) {
    try {
      const runRes = await fetch(`https://api.apify.com/v2/acts/${actor.id}/runs?waitForFinish=120`, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json', 'Authorization': `Bearer ${token}` },
        body: JSON.stringify(actor.input),
      });
      if (!runRes.ok) { log('APIFY', `Actor ${actor.id} failed (${runRes.status}), trying next...`); continue; }

      const run = await runRes.json();
      const datasetId = run.data?.defaultDatasetId;
      if (!datasetId) continue;

      const dataRes = await fetch(`https://api.apify.com/v2/datasets/${datasetId}/items?limit=30`, {
        headers: { 'Authorization': `Bearer ${token}` },
      });
      const items = await dataRes.json();
      if (!Array.isArray(items) || items.length === 0) continue;

      const articles = items.map(a => ({
        title: a.title || a.name || '',
        source: a.source || a.publisher || a.displayedUrl || '',
        url: a.url || a.link || '',
        date: a.publishedAt || a.date || a.datePublished || new Date().toISOString(),
        snippet: a.description || a.snippet || a.text || '',
      })).filter(a => a.title);

      if (articles.length > 0) {
        log('APIFY', `Done: ${articles.length} articles via ${actor.id}`);
        return articles;
      }
    } catch (e) { log('APIFY', `Actor ${actor.id} error: ${e.message}`); }
  }

  log('APIFY', 'All actors failed, no news data');
  return null;
}

// ===== 3. EUMETRA — Consumer Survey =====
async function fetchEumetraData() {
  log('EUMETRA', 'Fetching consumer survey data...');
  const { url: dbUrl, token } = CONFIG.turso;
  if (!dbUrl || !token) { log('EUMETRA', 'SKIP: No credentials'); return null; }

  try {
    // Use Turso HTTP API
    const tursoUrl = dbUrl.replace('libsql://', 'https://');

    async function runSQL(sql) {
      const res = await fetch(`${tursoUrl}/v2/pipeline`, {
        method: 'POST',
        headers: {
          'Content-Type': 'application/json',
          'Authorization': `Bearer ${token}`,
        },
        body: JSON.stringify({
          requests: [{ type: 'execute', stmt: { sql } }, { type: 'close' }],
        }),
      });
      if (!res.ok) throw new Error(`Turso ${res.status}`);
      const data = await res.json();
      const result = data.results?.[0]?.response?.result;
      if (!result) return [];
      const cols = result.cols.map(c => c.name);
      return result.rows.map(row => {
        const obj = {};
        row.forEach((cell, i) => { obj[cols[i]] = cell.value; });
        return obj;
      });
    }

    // Fetch food/alimentazione related questions
    const foodQuestions = await runSQL(`
      SELECT q.code, q.text, r.label, res.percentage
      FROM questions q
      JOIN responses r ON r.question_id = q.id
      JOIN results res ON res.response_id = r.id
      JOIN segments s ON s.id = res.segment_id
      JOIN categories c ON c.id = s.category_id
      WHERE c.name = 'TOTALE'
      AND (q.text LIKE '%pasta%' OR q.text LIKE '%alimenta%' OR q.text LIKE '%cibo%' OR q.text LIKE '%marca%' OR q.text LIKE '%brand%' OR q.text LIKE '%qualit%')
      AND res.percentage > 0
      ORDER BY res.percentage DESC
      LIMIT 50
    `);

    // Fetch wellness/lifestyle data
    const lifestyleData = await runSQL(`
      SELECT q.code, q.text, r.label, res.percentage
      FROM questions q
      JOIN responses r ON r.question_id = q.id
      JOIN results res ON res.response_id = r.id
      JOIN segments s ON s.id = res.segment_id
      JOIN categories c ON c.id = s.category_id
      WHERE c.name = 'TOTALE'
      AND (q.text LIKE '%sostenibil%' OR q.text LIKE '%bio%' OR q.text LIKE '%salut%' OR q.text LIKE '%benessere%')
      AND res.percentage > 0
      ORDER BY res.percentage DESC
      LIMIT 30
    `);

    log('EUMETRA', `Done: ${foodQuestions.length} food items, ${lifestyleData.length} lifestyle items`);

    return {
      surveyName: 'Eumetra Benessere 25/26',
      sampleSize: 29441,
      food: foodQuestions.map(q => ({
        question: q.text?.slice(0, 150),
        answer: q.label,
        percentage: +q.percentage,
      })),
      lifestyle: lifestyleData.map(q => ({
        question: q.text?.slice(0, 150),
        answer: q.label,
        percentage: +q.percentage,
      })),
    };
  } catch (err) {
    log('EUMETRA', `Error: ${err.message}`);
    return null;
  }
}

// ===== 4. CLAUDE API — AI Insights =====
async function generateInsights(socialData, newsData, consumerData) {
  log('CLAUDE', 'Generating AI insights...');
  const { apiKey } = CONFIG.anthropic;
  if (!apiKey) { log('CLAUDE', 'SKIP: No API key'); return null; }

  const dataContext = `
## SOCIAL DATA (OpenSearch - last 7 days)
- Total posts: ${socialData?.overview?.totalPosts || 'N/A'}
- Volume change vs previous period: ${socialData?.overview?.volumeChange || 'N/A'}%
- Avg engagement rate: ${((socialData?.overview?.avgEngagementRate || 0) * 100).toFixed(2)}%
- ER change: ${socialData?.overview?.erChange || 'N/A'}%
- Organic content: ${socialData?.overview?.organicPct || 'N/A'}%
- Brand Health Score: ${socialData?.brandHealth?.score || 'N/A'}/100
- Net Sentiment: +${socialData?.sentiment?.netScore || 'N/A'}
- Top platforms: ${socialData?.platforms?.map(p => `${p.name} (${p.count} posts, ${p.pctOfTotal}%)`).join(', ') || 'N/A'}
- Top hashtags: ${socialData?.hashtags?.slice(0, 10).map(h => `#${h.tag} (${h.count})`).join(', ') || 'N/A'}
- Top creators: ${socialData?.creators?.slice(0, 5).map(c => `${c.name} (${formatNum(c.engagement)} eng)`).join(', ') || 'N/A'}

## NEWS (Google News - recent)
${newsData?.slice(0, 10).map(n => `- [${n.source}] ${n.title}`).join('\n') || 'No news data available'}

## CONSUMER DATA (Eumetra Survey - Italian population)
${consumerData?.food?.slice(0, 10).map(f => `- ${f.answer}: ${f.percentage}% (${f.question?.slice(0, 80)})`).join('\n') || 'No consumer data available'}
`;

  try {
    const res = await fetch('https://api.anthropic.com/v1/messages', {
      method: 'POST',
      headers: {
        'Content-Type': 'application/json',
        'x-api-key': apiKey,
        'anthropic-version': '2023-06-01',
      },
      body: JSON.stringify({
        model: 'claude-sonnet-4-20250514',
        max_tokens: 2000,
        messages: [{
          role: 'user',
          content: `Sei un analista di brand reputation per Barilla. Basandoti sui dati seguenti, genera un JSON con 4 insight e 5 forecast per la dashboard di monitoraggio.

${dataContext}

Rispondi SOLO con un JSON valido in questo formato esatto:
{
  "insights": [
    {"type": "brand_brief", "title": "BRAND BRIEF", "text": "...", "severity": "green"},
    {"type": "attention", "title": "SEGNALE DI ATTENZIONE", "text": "...", "severity": "amber"},
    {"type": "opportunity", "title": "OPPORTUNITÀ", "text": "...", "severity": "blue"},
    {"type": "competitor", "title": "COMPETITOR WATCH", "text": "...", "severity": "purple"}
  ],
  "forecasts": [
    {"title": "...", "probability": 82, "color": "green", "tags": ["BRAND","SOCIAL"], "meta": "24h: 85% | 7d: 82%"},
    {"title": "...", "probability": 41, "color": "amber", "tags": ["RISCHIO"], "meta": "..."},
    {"title": "...", "probability": 67, "color": "cyan", "tags": ["TIKTOK"], "meta": "..."},
    {"title": "...", "probability": 29, "color": "red", "tags": ["COMPETITOR"], "meta": "..."},
    {"title": "...", "probability": 94, "color": "green", "tags": ["EVENTO"], "meta": "..."}
  ],
  "escalations": [
    {"name": "...", "detail": "...", "level": "med", "signals": 47},
    {"name": "...", "detail": "...", "level": "low", "signals": 12}
  ]
}

Basa insight e forecast sui dati reali forniti. Sii specifico e usa numeri reali.`
        }],
      }),
    });

    if (!res.ok) throw new Error(`Claude API ${res.status}`);
    const data = await res.json();
    const text = data.content?.[0]?.text || '';

    // Extract JSON from response
    const jsonMatch = text.match(/\{[\s\S]*\}/);
    if (!jsonMatch) throw new Error('No JSON in response');

    const insights = JSON.parse(jsonMatch[0]);
    log('CLAUDE', `Done: ${insights.insights?.length} insights, ${insights.forecasts?.length} forecasts`);
    return insights;
  } catch (err) {
    log('CLAUDE', `Error: ${err.message}`);
    return null;
  }
}

// ===== 5. COMPETITOR DATA =====
async function fetchCompetitorData(days = 7) {
  log('COMPETITORS', 'Fetching competitor share of voice...');
  const { url, index, user, pass } = CONFIG.opensearch;
  if (!user || !pass) return null;

  const auth = Buffer.from(`${user}:${pass}`).toString('base64');
  const from = new Date(Date.now() - days * 86400000).toISOString();
  const brands = ['barilla', ...CONFIG.brand.competitors];

  // Get total posts in the period (the index is Barilla-focused, so all posts = Barilla)
  const totalRes = await fetch(`${url}/${index}/_count`, {
    method: 'POST',
    headers: { 'Content-Type': 'application/json', 'Authorization': `Basic ${auth}` },
    body: JSON.stringify({ query: { range: { published_at: { gte: from } } } }),
  });
  const totalCount = totalRes.ok ? (await totalRes.json()).count : 0;

  const results = [{ name: 'Barilla', count: totalCount }];
  for (const brand of CONFIG.brand.competitors) {
    try {
      const res = await fetch(`${url}/${index}/_count`, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json', 'Authorization': `Basic ${auth}` },
        body: JSON.stringify({
          query: {
            bool: {
              must: [
                { match_phrase: { caption: brand } },
                { range: { published_at: { gte: from } } }
              ]
            }
          }
        }),
      });
      if (!res.ok) continue;
      const data = await res.json();
      results.push({ name: brand.charAt(0).toUpperCase() + brand.slice(1), count: data.count });
    } catch (e) { /* skip */ }
  }

  const total = results.reduce((s, r) => s + r.count, 0) || 1;
  const competitors = results.map(r => ({
    name: r.name,
    mentions: r.count,
    shareOfVoice: +((r.count / total) * 100).toFixed(1),
  }));

  log('COMPETITORS', `Done: ${competitors.length} brands tracked`);
  return competitors;
}

// ===== MAIN PIPELINE =====
async function main() {
  log('PIPELINE', '=== Barilla Monitor Data Pipeline Starting ===');
  const startTime = Date.now();

  // Run all data fetches in parallel where possible
  const [socialData, newsData, consumerData, competitorData] = await Promise.all([
    fetchOpenSearchData(7).catch(e => { log('OPENSEARCH', `FATAL: ${e.message}`); return null; }),
    fetchGoogleNews().catch(e => { log('APIFY', `FATAL: ${e.message}`); return null; }),
    fetchEumetraData().catch(e => { log('EUMETRA', `FATAL: ${e.message}`); return null; }),
    fetchCompetitorData(7).catch(e => { log('COMPETITORS', `FATAL: ${e.message}`); return null; }),
  ]);

  // Generate AI insights (depends on collected data)
  const aiInsights = await generateInsights(socialData, newsData, consumerData)
    .catch(e => { log('CLAUDE', `FATAL: ${e.message}`); return null; });

  // Assemble final data.json
  const output = {
    meta: {
      generatedAt: new Date().toISOString(),
      pipelineVersion: '1.0.0',
      sources: {
        opensearch: !!socialData,
        apify: !!newsData,
        eumetra: !!consumerData,
        claude: !!aiInsights,
        competitors: !!competitorData,
      },
      processingTimeMs: Date.now() - startTime,
    },
    social: socialData,
    news: newsData,
    consumer: consumerData,
    ai: aiInsights,
    competitors: competitorData,
  };

  // Write output
  const outputPath = process.env.OUTPUT_PATH || 'data.json';
  fs.writeFileSync(outputPath, JSON.stringify(output, null, 2));
  log('PIPELINE', `=== Pipeline complete in ${((Date.now() - startTime) / 1000).toFixed(1)}s ===`);
  log('PIPELINE', `Output: ${outputPath} (${(fs.statSync(outputPath).size / 1024).toFixed(1)} KB)`);
  log('PIPELINE', `Sources: ${Object.entries(output.meta.sources).filter(([, v]) => v).map(([k]) => k).join(', ')}`);
}

main().catch(err => {
  console.error('Pipeline failed:', err);
  process.exit(1);
});
