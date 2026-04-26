using System;
using System.Collections.Generic;
using System.Globalization;
using System.Linq;
using System.Text.RegularExpressions;
using System.Threading;
using System.Threading.Tasks;
using AngleSharp.Dom;
using AngleSharp.Html.Parser;
using Jackett.Common.Extensions;
using Jackett.Common.Indexers.Definitions.Abstract;
using Jackett.Common.Models;
using Jackett.Common.Services.Interfaces;
using Jackett.Common.Utils.Clients;
using NLog;
using WebClient = Jackett.Common.Utils.Clients.WebClient;

namespace Jackett.Common.Indexers.Definitions
{
    public class RedeTorrent : PublicBrazilianIndexerBase
    {
        public override string Id => "redetorrent";
        public override string Name => "RedeTorrent";
        public override string SiteLink { get; protected set; } = "https://redetorrent.com/";

        private TorznabCapabilities _cachedCaps;

        public RedeTorrent(IIndexerConfigurationService configService, WebClient wc, Logger l, IProtectionService ps, ICacheService cs)
            : base(configService, wc, l, ps, cs)
        {
        }

        public override TorznabCapabilities TorznabCaps
        {
            get
            {
                if (_cachedCaps != null)
                    return _cachedCaps;
                var caps = base.TorznabCaps;
                caps.Categories.AddCategoryMapping("desenhos", TorznabCatType.TVAnime);
                _cachedCaps = caps;
                return _cachedCaps;
            }
        }

        public override IParseIndexerResponse GetParser() => new RedeTorrentParser(webclient, logger);

        public override IIndexerRequestGenerator GetRequestGenerator() => new RedeTorrentRequestGenerator(SiteLink);
    }

    public class RedeTorrentRequestGenerator : IIndexerRequestGenerator
    {
        private readonly string _siteLink;

        public RedeTorrentRequestGenerator(string siteLink)
        {
            _siteLink = siteLink;
        }

        public IndexerPageableRequestChain GetSearchRequests(TorznabQuery query)
        {
            var chain = new IndexerPageableRequestChain();
            var term = query.SearchTerm ?? string.Empty;
            if (query.Season is { } season)
                term = $"{term} {season}".Trim();
            var url = string.IsNullOrWhiteSpace(term)
                ? _siteLink
                : $"{_siteLink}index.php?s={System.Net.WebUtility.UrlEncode(term)}";
            chain.Add(new[] { new IndexerRequest(url) });
            return chain;
        }
    }

    public class RedeTorrentParser : PublicBrazilianParser
    {
        private const int MaxConcurrentRequests = 2;

        private readonly WebClient _webclient;
        private readonly Logger _logger;

        public RedeTorrentParser(WebClient webclient, Logger logger)
        {
            _webclient = webclient;
            _logger = logger;
        }

        private struct ListingMeta
        {
            public int CategoryId;
            public List<string> Languages;
            public string QualityFromBadge;
            public DateTime? PublishDate;
            public string TitleFromAnchor;
        }

        private static int MapListingCategory(string text) => text?.Trim().ToLowerInvariant() switch
        {
            "desenho" or "desenhos" or "anime" or "animes" => TorznabCatType.TVAnime.ID,
            "filme" or "filmes" => TorznabCatType.Movies.ID,
            "série" or "séries" or "serie" or "series" => TorznabCatType.TV.ID,
            _ => 0
        };

        private static Dictionary<string, string> ExtractFileInfo(IDocument detailsDom)
        {
            var fileInfo = new Dictionary<string, string>(StringComparer.OrdinalIgnoreCase);
            var infoSection = detailsDom.QuerySelector("#informacoes p");
            if (infoSection == null)
                return fileInfo;

            var lines = infoSection.InnerHtml.Split(new[] { "<br>" }, StringSplitOptions.RemoveEmptyEntries);
            foreach (var line in lines)
            {
                if (!line.Contains("<strong>") || !line.Contains(":"))
                    continue;

                var cleanLine = Regex.Replace(line, @"<[^>]+>", string.Empty);
                cleanLine = System.Net.WebUtility.HtmlDecode(cleanLine);
                cleanLine = Regex.Replace(cleanLine, @"\s+", " ");
                var parts = cleanLine.Split(new[] { ':' }, 2);
                if (parts.Length != 2)
                    continue;

                var key = parts[0].Trim();
                var value = parts[1].Trim().Trim('/', ',', '|').Trim();
                if (string.IsNullOrEmpty(key) || string.IsNullOrEmpty(value))
                    continue;

                value = value switch
                {
                    var v when v.Contains("Dual Áudio") => v.Replace("Dual Áudio", "Dual"),
                    var v when v.Contains("Dual Audio") => v.Replace("Dual Audio", "Dual"),
                    var v when v.Contains("Full HD") => v.Replace("Full HD", "1080p"),
                    var v when v.Contains("4K") => v.Replace("4K", "2160p"),
                    var v when v.Contains("SD") => v.Replace("SD", "480p"),
                    var v when v.Contains("WEB-DL") => v,
                    var v when v.Contains("WEB") => v.Replace("WEB", "WEB-DL"),
                    _ => value
                };

                fileInfo[key] = value;
            }

            return NormalizeRedeTorrentKeys(fileInfo);
        }

        private static Dictionary<string, string> NormalizeRedeTorrentKeys(Dictionary<string, string> raw)
        {
            var canonical = new Dictionary<string, string>(raw, StringComparer.OrdinalIgnoreCase);
            AliasKey(canonical, "Gêneros", "Gênero");
            AliasKey(canonical, "Generos", "Gênero");
            AliasKey(canonical, "Legendas", "Legenda");
            AliasKey(canonical, "Titulo Original", "Título Original");
            AliasKey(canonical, "Titulo Traduzido", "Título Traduzido");
            AliasKey(canonical, "Nota do Imdb", "IMDb");
            AliasKey(canonical, "Nota do IMDb", "IMDb");
            return canonical;
        }

        private static void AliasKey(Dictionary<string, string> dict, string aliasKey, string canonicalKey)
        {
            if (dict.TryGetValue(aliasKey, out var value) && !dict.ContainsKey(canonicalKey))
                dict[canonicalKey] = value;
            dict.Remove(aliasKey);
        }

        public override IList<ReleaseInfo> ParseResponse(IndexerResponse indexerResponse)
        {
            var firstPageUri = new Uri(indexerResponse.Request.Url);
            using var firstDom = new HtmlParser().ParseDocument(indexerResponse.Content);

            var listingDocs = new List<(Uri PageUri, IDocument Document)> { (firstPageUri, firstDom) };
            List<(Uri Uri, IDocument Document)> extraDocs = null;
            List<(Uri Uri, IDocument Document)> detailDocs = null;

            try
            {
                var extraPageUris = GetExtraPageUris(firstDom, firstPageUri);
                if (extraPageUris.Count > 0)
                {
                    extraDocs = FetchDocumentsAsync(extraPageUris).GetAwaiter().GetResult();
                    listingDocs.AddRange(extraDocs.Select(t => (t.Uri, t.Document)));
                }

                var searchRows = new List<(Uri DetailUrl, ListingMeta Meta)>();
                var seenDetailUrls = new HashSet<Uri>();
                foreach (var (pageUri, doc) in listingDocs)
                {
                    foreach (var row in doc.QuerySelectorAll("div.capas_pequenas div.capa_lista"))
                    {
                        var detailAnchor = row.QuerySelector("a[href]");
                        var href = detailAnchor?.GetAttribute("href");
                        if (string.IsNullOrWhiteSpace(href))
                            continue;
                        Uri detailUrl;
                        try
                        {
                            detailUrl = new Uri(pageUri, href);
                        }
                        catch
                        {
                            continue;
                        }
                        if (!seenDetailUrls.Add(detailUrl))
                            continue;
                        searchRows.Add((detailUrl, BuildListingMeta(row, detailAnchor)));
                    }
                }

                detailDocs = FetchDocumentsAsync(searchRows.Select(r => r.DetailUrl).ToList())
                    .GetAwaiter().GetResult();
                var detailLookup = detailDocs.ToDictionary(t => t.Uri, t => t.Document);

                var releasesByMagnet = new Dictionary<Uri, ReleaseInfo>();
                var perRowReleases = new List<ReleaseInfo>();
                foreach (var (detailUrl, meta) in searchRows)
                {
                    if (!detailLookup.TryGetValue(detailUrl, out var detailsDom))
                        continue;
                    perRowReleases.Clear();
                    BuildReleases(detailUrl, detailsDom, meta, perRowReleases);
                    foreach (var release in perRowReleases)
                    {
                        if (release.MagnetUri != null && !releasesByMagnet.ContainsKey(release.MagnetUri))
                            releasesByMagnet.Add(release.MagnetUri, release);
                    }
                }

                return releasesByMagnet.Values.ToList();
            }
            finally
            {
                if (extraDocs != null)
                    foreach (var (_, doc) in extraDocs)
                        doc?.Dispose();
                if (detailDocs != null)
                    foreach (var (_, doc) in detailDocs)
                        doc?.Dispose();
            }
        }

        private static ListingMeta BuildListingMeta(IElement row, IElement detailAnchor)
        {
            var meta = new ListingMeta
            {
                CategoryId = MapListingCategory(row.QuerySelector("span.capa_categoria")?.TextContent)
            };

            var qualityBadge = row.QuerySelector("span.capa_qualidade")?.TextContent?.Trim();
            if (!string.IsNullOrWhiteSpace(qualityBadge))
            {
                var match = Regex.Match(qualityBadge, @"\d{3,4}P", RegexOptions.IgnoreCase);
                if (match.Success)
                    meta.QualityFromBadge = match.Value.ToLowerInvariant();
            }

            var idiomaText = row.QuerySelector("span.capa_idioma")?.TextContent?.Trim();
            if (!string.IsNullOrWhiteSpace(idiomaText))
                meta.Languages = new List<string> { idiomaText };

            var datetime = row.QuerySelector("time[itemprop='dateModified']")?.GetAttribute("datetime");
            if (!string.IsNullOrWhiteSpace(datetime) &&
                DateTime.TryParse(datetime, CultureInfo.InvariantCulture,
                    DateTimeStyles.AssumeUniversal | DateTimeStyles.AdjustToUniversal, out var parsedDate))
                meta.PublishDate = parsedDate;

            meta.TitleFromAnchor = row.QuerySelector("h2[itemprop='headline']")?.TextContent?.Trim()
                                   ?? detailAnchor?.GetAttribute("title")?.Trim();
            return meta;
        }

        private void BuildReleases(Uri detailUrl, IDocument detailsDom, ListingMeta meta, List<ReleaseInfo> releases)
        {
            var fileInfoDict = ExtractFileInfo(detailsDom);
            var fileInfo = PublicBrazilianIndexerBase.FileInfo.FromDictionary(fileInfoDict);

            DateTime publishDate;
            if (meta.PublishDate.HasValue)
                publishDate = meta.PublishDate.Value;
            else if (!string.IsNullOrEmpty(fileInfo.ReleaseYear) &&
                     DateTime.TryParseExact(fileInfo.ReleaseYear, "yyyy", CultureInfo.InvariantCulture, DateTimeStyles.None, out var parsedYear))
                publishDate = parsedYear;
            else
                publishDate = DateTime.Today;

            var detailTitle = detailsDom.QuerySelector("h1[itemprop='headline']")?.TextContent?.Trim();
            var rawDefaultTitle = !string.IsNullOrWhiteSpace(detailTitle) ? detailTitle : meta.TitleFromAnchor;
            var defaultTitle = CleanTitle(rawDefaultTitle ?? string.Empty);
            if (string.IsNullOrWhiteSpace(defaultTitle) && !string.IsNullOrWhiteSpace(fileInfo.TitleOriginal))
                defaultTitle = CleanTitle(fileInfo.TitleOriginal);
            if (string.IsNullOrWhiteSpace(defaultTitle) && !string.IsNullOrWhiteSpace(fileInfo.TitleTranslated))
                defaultTitle = CleanTitle(fileInfo.TitleTranslated);

            var resolution = fileInfo.Quality ?? fileInfo.VideoQuality ?? meta.QualityFromBadge ?? string.Empty;

            var magnetLinks = detailsDom.QuerySelectorAll("p#lista_download a.btn[href^=\"magnet:\"]");
            foreach (var magnetLink in magnetLinks)
            {
                var magnetHref = magnetLink.GetAttribute("href");
                if (string.IsNullOrWhiteSpace(magnetHref))
                    continue;

                Uri magnetUri;
                try
                {
                    magnetUri = new Uri(magnetHref);
                }
                catch
                {
                    continue;
                }

                var release = new ReleaseInfo
                {
                    Details = detailUrl,
                    Guid = magnetUri,
                    MagnetUri = magnetUri,
                    PublishDate = publishDate,
                    Seeders = 1,
                    DownloadVolumeFactor = 0,
                    UploadVolumeFactor = 1,
                    Languages = fileInfo.Audio?.ToList() ?? meta.Languages,
                    Genres = fileInfo.Genres?.ToList(),
                    Subs = string.IsNullOrEmpty(fileInfo.Subtitle) ? null : new[] { fileInfo.Subtitle }
                };

                var titleWithResolution = string.IsNullOrWhiteSpace(resolution)
                    ? defaultTitle
                    : $"{defaultTitle} {resolution}".Trim();
                release.Title = ExtractTitleOrDefault(magnetLink, titleWithResolution);

                release.Category = meta.CategoryId > 0
                    ? new List<int> { meta.CategoryId }
                    : magnetLink.ExtractCategory(release.Title);
                release.Size = !string.IsNullOrWhiteSpace(fileInfo.Size)
                    ? RowParsingExtensions.GetBytes(fileInfo.Size)
                    : ExtractSizeByResolution(release.Title);

                if (release.Title.IsNotNullOrWhiteSpace())
                    releases.Add(release);
            }
        }

        private static List<Uri> GetExtraPageUris(IDocument dom, Uri firstPageUri)
        {
            var seen = new HashSet<Uri>();
            var pages = new List<(int Page, Uri Uri)>();
            var activePages = new HashSet<int>();

            foreach (var link in dom.QuerySelectorAll("div.paginacao ul.pagination li.page-item a.page-link"))
            {
                var title = link.GetAttribute("title") ?? string.Empty;
                var match = Regex.Match(title, @"^Pagina\s+(\d+)$", RegexOptions.IgnoreCase);
                if (!match.Success || !int.TryParse(match.Groups[1].Value, out var pageNumber))
                    continue;

                var parentClass = link.ParentElement?.GetAttribute("class") ?? string.Empty;
                if (parentClass.IndexOf("active", StringComparison.OrdinalIgnoreCase) >= 0)
                {
                    activePages.Add(pageNumber);
                    continue;
                }

                var href = link.GetAttribute("href");
                if (string.IsNullOrWhiteSpace(href))
                    continue;

                Uri uri;
                try
                {
                    uri = new Uri(firstPageUri, href);
                }
                catch
                {
                    continue;
                }

                if (uri.AbsoluteUri.Equals(firstPageUri.AbsoluteUri, StringComparison.OrdinalIgnoreCase))
                    continue;
                if (!seen.Add(uri))
                    continue;
                pages.Add((pageNumber, uri));
            }

            return pages
                .Where(p => !activePages.Contains(p.Page))
                .OrderBy(p => p.Page)
                .Select(p => p.Uri)
                .ToList();
        }

        private async Task<List<(Uri Uri, IDocument Document)>> FetchDocumentsAsync(IReadOnlyCollection<Uri> uris)
        {
            if (uris.Count == 0)
                return new List<(Uri, IDocument)>();

            using var semaphore = new SemaphoreSlim(MaxConcurrentRequests);
            var tasks = uris.Select(async uri =>
            {
                await semaphore.WaitAsync().ConfigureAwait(false);
                try
                {
                    var response = await _webclient.GetResultAsync(new WebRequest(uri.ToString())).ConfigureAwait(false);
                    if (response == null || string.IsNullOrEmpty(response.ContentString))
                    {
                        _logger?.Warn($"RedeTorrent: empty response from {uri} (status {response?.Status.ToString() ?? "n/a"})");
                        IDocument empty = new HtmlParser().ParseDocument(string.Empty);
                        return (uri, empty);
                    }
                    if ((int)response.Status >= 400)
                        _logger?.Warn($"RedeTorrent: HTTP {(int)response.Status} from {uri}");
                    IDocument document = new HtmlParser().ParseDocument(response.ContentString);
                    return (uri, document);
                }
                catch (Exception ex)
                {
                    _logger?.Warn(ex, $"RedeTorrent: failed to fetch {uri}");
                    IDocument empty = new HtmlParser().ParseDocument(string.Empty);
                    return (uri, empty);
                }
                finally
                {
                    semaphore.Release();
                }
            });
            var results = await Task.WhenAll(tasks).ConfigureAwait(false);
            return results.ToList();
        }

        protected override INode GetTitleElementOrNull(IElement downloadButton)
        {
            var description = downloadButton.PreviousSibling;
            while (description != null && description.NodeType != NodeType.Text)
            {
                description = description.PreviousSibling;
            }

            return description;
        }
    }
}
