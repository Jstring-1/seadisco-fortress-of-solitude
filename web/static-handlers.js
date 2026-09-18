// GENERATED from the inline on*="…" handlers that used to live in
// index.html, admin-panel.html and admin.html, so pages can run under a
// Content-Security-Policy without 'unsafe-inline'. Markup references
// these as data-sd-<event>="@key"; shared.js binds them on first use
// (see _sdOn / _sdBindOnPath). Each runs with `this` = the element and
// `event` = the event, exactly like the inline handler did. Edit the
// code here; new markup should use data-sd-<event>="@key" too.
window._sdStatic = Object.assign(window._sdStatic || {}, {
  // index.html:128 <input id="query">
  i128_input: function (event) { window.currentBarcode=null },
  // index.html:131 <button id="search-btn">
  i131_click: function (event) { doSearch(1) },
  // index.html:137 <button id="card-mode-toggle">
  i137_click: function (event) { _sdToggleCardMode() },
  // index.html:142 <button id="barcode-scan-btn">
  i142_click: function (event) { if(window._sdOpenScanner){window._sdOpenScanner()}else if(window._sdLoadModule){window._sdLoadModule('/scanner.js').then(()=>window._sdOpenScanner&&window._sdOpenScanner())} },
  // index.html:157 <button id="swap-to-collection-btn">
  i157_click: function (event) { swapSearchToCollection() },
  // index.html:163 <button id="advanced-toggle">
  i163_click: function (event) { toggleAdvanced() },
  // index.html:190 <button id="hide-owned">
  i190_click: function (event) { _sdHideOwnedToggled(this) },
  // index.html:199 <button id="f-hard2find">
  i199_click: function (event) { _sdHard2FindChanged(this) },
  // index.html:205 <button id="f-exclude-cd">
  i205_click: function (event) { _sdToggleExcludeCd(this) },
  // index.html:213 <button id="f-noyear">
  i213_click: function (event) { _sdToggleHideNoYear() },
  // index.html:223 <input id="f-artist">
  i223_input: function (event) { window.currentArtistId=null;window.currentBarcode=null },
  // index.html:225 <input id="f-label">
  i225_input: function (event) { currentLabelId=null;window.currentBarcode=null },
  // index.html:260 <select id="f-genre">
  i260_change: function (event) { populateStyles() },
  // index.html:278 <button id="f-genre-strict">
  i278_click: function (event) { _sdToggleGenreStrict('search', this) },
  // index.html:393 <input id="cw-query">
  i393_keydown: function (event) { if(event.key==='Enter'){doCwSearch(1)} },
  // index.html:398 <button>
  i398_click: function (event) { doCwSearch(1) },
  // index.html:400 <button id="cw-card-mode-toggle">
  i400_click: function (event) { _sdToggleCardMode() },
  // index.html:401 <button>
  i401_click: function (event) { swapSearchToMain() },
  // index.html:406 <button id="cw-advanced-toggle">
  i406_click: function (event) { toggleCwAdvanced() },
  // index.html:411 <input>
  i411_change: function (event) { doCwSearch(1) },
  // index.html:412 <input>
  i412_change: function (event) { doCwSearch(1) },
  // index.html:413 <input>
  i413_change: function (event) { doCwSearch(1) },
  // index.html:414 <input>
  i414_change: function (event) { doCwSearch(1) },
  // index.html:419 <select id="cw-sort">
  i419_change: function (event) { saveCwSort();doCwSearch(1) },
  // index.html:430 <button id="cw-export-btn">
  i430_click: function (event) { _cwExport() },
  // index.html:434 <button id="cw-noyear">
  i434_click: function (event) { _sdToggleHideNoYear() },
  // index.html:446 <input id="cw-artist">
  i446_keydown: function (event) { if(event.key==='Enter'){doCwSearch(1)} },
  // index.html:447 <input id="cw-release">
  i447_keydown: function (event) { if(event.key==='Enter'){doCwSearch(1)} },
  // index.html:448 <input id="cw-label">
  i448_keydown: function (event) { if(event.key==='Enter'){doCwSearch(1)} },
  // index.html:449 <input id="cw-notes">
  i449_keydown: function (event) { if(event.key==='Enter'){doCwSearch(1)} },
  // index.html:451 <select id="cw-format">
  i451_change: function (event) { doCwSearch(1) },
  // index.html:480 <input id="cw-year">
  i480_keydown: function (event) { if(event.key==='Enter'){doCwSearch(1)} },
  // index.html:483 <select id="cw-genre">
  i483_change: function (event) { onCwGenreChange() },
  // index.html:486 <button id="cw-genre-strict">
  i486_click: function (event) { _sdToggleGenreStrict('cw', this) },
  // index.html:493 <select id="cw-style">
  i493_change: function (event) { doCwSearch(1) },
  // index.html:498 <select id="cw-rating">
  i498_change: function (event) { doCwSearch(1) },
  // index.html:517 <input id="wanted-q">
  i517_input: function (event) { filterWantedItems() },
  // index.html:519 <button>
  i519_click: function (event) { document.getElementById('wanted-q').value='';filterWantedItems() },
  // index.html:525 <div id="alts-popup-backdrop">
  i525_click: function (event) { closeAltsPopup() },
  // index.html:529 <button id="search-load-more-btn">
  i529_click: function (event) { loadMoreResults() },
  // index.html:537 <button>
  i537_click: function (event) { openSignUpModal() },
  // index.html:538 <button>
  i538_click: function (event) { openSignInModal() },
  // index.html:541 <a>
  i541_click: function (event) { switchView('privacy');return false; },
  // index.html:541 <a>
  i541_click_2: function (event) { switchView('terms');return false; },
  // index.html:554 <span id="rr-tab-recent">
  i554_click: function (event) { _sdSwitchHomeStripTab('recent') },
  // index.html:556 <span id="rr-tab-feed">
  i556_click: function (event) { _sdSwitchHomeStripTab('feed') },
  // index.html:558 <span id="rr-tab-active">
  i558_click: function (event) { _sdSwitchHomeStripTab('active') },
  // index.html:560 <span id="rr-tab-played">
  i560_click: function (event) { _sdSwitchHomeStripTab('played') },
  // index.html:562 <span id="rr-tab-rare">
  i562_click: function (event) { _sdSwitchHomeStripTab('rare') },
  // index.html:564 <span id="rr-tab-dig">
  i564_click: function (event) { _sdSwitchHomeStripTab('dig') },
  rr_tab_blues_click: function (event) { _sdSwitchHomeStripTab('blues') },
  // index.html:570 <input id="random-records-filter">
  i570_input: function (event) { _sdHomeStripFilterChanged(this) },
  // index.html:573 <select id="random-records-genre">
  i573_change: function (event) { _sdHomeStripGenreChanged(this) },
  // index.html:576 <button id="random-records-genre-strict">
  i576_click: function (event) { _sdToggleGenreStrict('strip', this) },
  // index.html:586 <select id="favorites-sort">
  i586_change: function (event) { sortFavoritesGrid() },
  // index.html:598 <button id="rr-noyear">
  i598_click: function (event) { _sdToggleHideNoYear() },
  // index.html:602 <button id="recent-clear-btn">
  i602_click: function (event) { _sdHomeStripClearFilters() },
  // index.html:722 <a>
  i722_click: function (event) { switchView('account');return false; },
  // index.html:769 <a>
  i769_click: function (event) { switchView('account');return false; },
  // index.html:779 <span id="extras-tab-loc">
  i779_click: function (event) { switchView('loc') },
  // index.html:781 <span id="extras-tab-archive">
  i781_click: function (event) { switchView('archive') },
  // index.html:783 <span id="extras-tab-wiki">
  i783_click: function (event) { switchView('wiki') },
  // index.html:787 <span id="extras-tab-gutenberg">
  i787_click: function (event) { switchView('gutenberg') },
  // index.html:791 <span id="extras-tab-chronam">
  i791_click: function (event) { switchView('chronam') },
  // index.html:795 <span id="extras-tab-blues-archive">
  i795_click: function (event) { switchView('blues-archive') },
  // index.html:798 <span id="extras-tab-youtube">
  i798_click: function (event) { switchView('youtube') },
  // index.html:832 <button>
  i832_click: function (event) { _archiveSwitchTab('search') },
  // index.html:833 <button>
  i833_click: function (event) { _archiveSwitchTab('saved') },
  // index.html:834 <button>
  i834_click: function (event) { _archiveSwitchTab('curated') },
  // index.html:847 <button>
  i847_click: function (event) { _wikiSwitchTab('search') },
  // index.html:848 <button>
  i848_click: function (event) { _wikiSwitchTab('saved') },
  // index.html:853 <form>
  i853_submit: function (event) { event.preventDefault();runWikiPageSearch(document.getElementById('wiki-view-q').value) },
  // index.html:879 <button>
  i879_click: function (event) { _youtubeSwitchTab('search') },
  // index.html:880 <button>
  i880_click: function (event) { _youtubeSwitchTab('saved') },
  // index.html:885 <form>
  i885_submit: function (event) { event.preventDefault();runYoutubeSearch(document.getElementById('youtube-view-q').value) },
  // index.html:898 <input>
  i898_input: function (event) { _youtubeOnSavedFilterInput(this) },
  // index.html:899 <select>
  i899_change: function (event) { _youtubeOnSavedSortChange(this) },
  // index.html:920 <button>
  i920_click: function (event) { _gutenbergSwitchTab('search') },
  // index.html:921 <button>
  i921_click: function (event) { _gutenbergSwitchTab('saved') },
  // index.html:926 <form>
  i926_submit: function (event) { event.preventDefault();runGutenbergSearch(document.getElementById('gutenberg-q').value) },
  // index.html:937 <select id="gutenberg-topic-picker">
  i937_change: function (event) { _gutenbergOnSubjectPick(this) },
  // index.html:1005 <button>
  i1005_click: function (event) { _chronamSwitchTab('search') },
  // index.html:1006 <button>
  i1006_click: function (event) { _chronamSwitchTab('saved') },
  // index.html:1011 <form>
  i1011_submit: function (event) { event.preventDefault();runChronAmSearch(document.getElementById('chronam-q').value) },
  // index.html:1023 <button>
  i1023_click: function (event) { _chronamToggleHelp() },
  // index.html:1086 <button>
  i1086_click: function (event) { _baSwitchSubtab('lyrics') },
  // index.html:1087 <button>
  i1087_click: function (event) { _baSwitchSubtab('tunings') },
  // index.html:1088 <button>
  i1088_click: function (event) { _baSwitchSubtab('export') },
  // index.html:1110 <select id="ba-export-source">
  i1110_change: function (event) { _baExportOnSourceChange() },
  // index.html:1118 <button id="ba-export-go">
  i1118_click: function (event) { _baExportRun() },
  // index.html:1139 <input id="blues-archive-lyrics-search">
  i1139_input: function (event) { _baLyricsDebouncedSearch() },
  // index.html:1140 <select id="blues-archive-lyrics-tuning">
  i1140_change: function (event) { _baLyricsApplyTuning() },
  // index.html:1143 <input id="blues-archive-lyrics-tuning-like">
  i1143_input: function (event) { _baLyricsDebouncedTuningLike() },
  // index.html:1145 <input id="blues-archive-lyrics-empty">
  i1145_change: function (event) { _baLyricsApplyEmpty() },
  // index.html:1149 <input id="blues-archive-lyrics-no-artist">
  i1149_change: function (event) { _baLyricsApplyNoArtist() },
  // index.html:1153 <input id="blues-archive-lyrics-pinned">
  i1153_change: function (event) { _baLyricsApplyPinned() },
  // index.html:1157 <input id="blues-archive-lyrics-favorites">
  i1157_change: function (event) { _baLyricsApplyFavorites() },
  // index.html:1161 <input id="blues-archive-lyrics-title-punct">
  i1161_change: function (event) { _baLyricsApplyTitlePunct() },
  // index.html:1165 <input id="blues-archive-lyrics-no-year">
  i1165_change: function (event) { _baLyricsApplyNoYear() },
  // index.html:1168 <button id="blues-archive-lyrics-resolve-years-cache-btn">
  i1168_click: function (event) { _baResolveYearsCache() },
  // index.html:1169 <button id="blues-archive-lyrics-resolve-years-discogs-btn">
  i1169_click: function (event) { _baResolveYearsDiscogs() },
  // index.html:1170 <button>
  i1170_click: function (event) { _baLoadLyrics();_baLoadStats(); },
  // index.html:1171 <button>
  i1171_click: function (event) { _baClearVisitedLyrics() },
  // index.html:1172 <button>
  i1172_click: function (event) { _baOpenBansOverlay() },
  // index.html:1173 <button>
  i1173_click: function (event) { _baOpenLyricEditor(null) },
  // index.html:1175 <button>
  i1175_click: function (event) { _baSwitchSubtab('export') },
  // index.html:1180 <button id="lyrics-recent-btn">
  i1180_click: function (event) { lyricsStartRecentRefresh() },
  // index.html:1184 <button id="lyrics-prescrape-btn">
  i1184_click: function (event) { lyricsStartPreScrape() },
  // index.html:1189 <button id="lyrics-scrape-btn">
  i1189_click: function (event) { lyricsFetchNewSinceLast() },
  // index.html:1191 <button id="lyrics-stop-btn">
  i1191_click: function (event) { lyricsStopScrape() },
  // index.html:1192 <button id="blues-archive-lyrics-clear">
  i1192_click: function (event) { _baLyricsClearFilters() },
  // index.html:1223 <input id="ba-tunings-search">
  i1223_input: function (event) { _baTuningsDebouncedSearch() },
  // index.html:1224 <select id="ba-tunings-artist">
  i1224_change: function (event) { _baTuningsApplyFilters() },
  // index.html:1225 <select id="ba-tunings-position">
  i1225_change: function (event) { _baTuningsApplyFilters() },
  // index.html:1226 <button>
  i1226_click: function (event) { _baOpenTuningAdd() },
  // index.html:1227 <button>
  i1227_click: function (event) { _baLoadTuningsGrid() },
  // index.html:1228 <button id="blues-export-tunings-btn">
  i1228_click: function (event) { _baExportTuningsCsv() },
  // index.html:1229 <button id="ba-tunings-clear">
  i1229_click: function (event) { _baTuningsClearFilters() },
  // index.html:1255 <div id="gutenberg-reader-overlay">
  i1255_click: function (event) { if(event.target===this)_gutenbergCloseReader() },
  // index.html:1260 <input id="gutenberg-reader-find">
  i1260_input: function (event) { _gutenbergFindOnInput(this) },
  // index.html:1262 <button id="gutenberg-find-prev">
  i1262_click: function (event) { _gutenbergFindStep(-1) },
  // index.html:1263 <button id="gutenberg-find-next">
  i1263_click: function (event) { _gutenbergFindStep(1) },
  // index.html:1264 <button>
  i1264_click: function (event) { _gutenbergAdjustFontSize(-1) },
  // index.html:1265 <button>
  i1265_click: function (event) { _gutenbergAdjustFontSize(1) },
  // index.html:1266 <button>
  i1266_click: function (event) { _gutenbergPinHere() },
  // index.html:1267 <button id="gutenberg-link-here-btn">
  i1267_click: function (event) { _gutenbergLinkHereStart() },
  // index.html:1268 <button>
  i1268_click: function (event) { _gutenbergCloseReader() },
  // index.html:1278 <div id="gutenberg-reader-body">
  i1278_scroll: function (event) { _gutenbergOnBodyScroll() },
  // index.html:1315 <button id="oauth-connect-btn">
  i1315_click: function (event) { startOAuth() },
  // index.html:1329 <button>
  i1329_click: function (event) { dismissMarketplaceBanner() },
  // index.html:1337 <button id="account-sync-btn">
  i1337_click: function (event) { accountSync() },
  // index.html:1345 <button id="orders-refresh-btn">
  i1345_click: function (event) { refreshOrders() },
  // index.html:1365 <button>
  i1365_click: function (event) { clearBrowsingData() },
  // index.html:1366 <button>
  i1366_click: function (event) { clearDataAndSignOut() },
  // index.html:1367 <button>
  i1367_click: function (event) { signOut() },
  // index.html:1368 <button>
  i1368_click: function (event) { openFeedback() },
  // index.html:1370 <button>
  i1370_click: function (event) { deleteAccount() },
  // index.html:1400 <button>
  i1400_click: function (event) { closeFeedback() },
  // index.html:1401 <button id="feedback-submit-btn">
  i1401_click: function (event) { submitFeedback() },
  // index.html:1412 <div id="blues-editor-overlay">
  i1412_click: function (event) { if(event.target===this)bluesDbCloseEditor() },
  // index.html:1416 <button>
  i1416_click: function (event) { bluesDbCloseEditor() },
  // index.html:1424 <form id="blues-editor-form">
  i1424_submit: function (event) { event.preventDefault();bluesDbSaveEditor() },
  // index.html:1449 <button>
  i1449_click: function (event) { bluesDbOpenLinkPicker() },
  // index.html:1462 <button>
  i1462_click: function (event) { bluesDbLoadReferences(_bluesDbState.editingId) },
  // index.html:1489 <button id="blues-editor-delete">
  i1489_click: function (event) { bluesDbDeleteFromEditor() },
  // index.html:1490 <button id="blues-editor-wiki">
  i1490_click: function (event) { bluesDbEnrichEditorWiki() },
  // index.html:1491 <button id="blues-editor-discogs">
  i1491_click: function (event) { bluesDbOpenDiscogsPicker() },
  // index.html:1492 <button id="blues-editor-discogs-refresh">
  i1492_click: function (event) { bluesDbRefreshFromDiscogs() },
  // index.html:1493 <button id="blues-editor-merge">
  i1493_click: function (event) { bluesDbOpenMergePicker() },
  // index.html:1494 <button id="blues-editor-yt">
  i1494_click: function (event) { bluesDbEnrichEditorYt() },
  // index.html:1495 <button>
  i1495_click: function (event) { bluesDbCloseEditor() },
  // index.html:1503 <div id="blues-discogs-picker">
  i1503_click: function (event) { if(event.target===this)bluesDbCloseDiscogsPicker() },
  // index.html:1508 <button>
  i1508_click: function (event) { bluesDbCloseDiscogsPicker() },
  // index.html:1511 <input id="blues-discogs-picker-q">
  i1511_keydown: function (event) { if(event.key==='Enter')bluesDbRunDiscogsPicker() },
  // index.html:1512 <button>
  i1512_click: function (event) { bluesDbRunDiscogsPicker() },
  // index.html:1521 <div id="blues-merge-picker">
  i1521_click: function (event) { if(event.target===this)bluesDbCloseMergePicker() },
  // index.html:1526 <button>
  i1526_click: function (event) { bluesDbCloseMergePicker() },
  // index.html:1532 <input id="blues-merge-picker-q">
  i1532_input: function (event) { bluesDbRunMergePickerDebounced() },
  // index.html:1533 <button>
  i1533_click: function (event) { bluesDbRunMergePicker() },
  // index.html:1545 <div>
  i1545_click: function (event) { closeModal() },
  // index.html:1545 <button>
  i1545_click_2: function (event) { event.stopPropagation();closeModal() },
  // index.html:1553 <div>
  i1553_click: function (event) { closeVersionPopup() },
  // index.html:1553 <button>
  i1553_click_2: function (event) { event.stopPropagation();closeVersionPopup() },
  // index.html:1561 <button id="series-close">
  i1561_click: function (event) { closeSeriesBrowser() },
  // index.html:1569 <button id="bio-full-close">
  i1569_click: function (event) { closeBioFull() },
  // index.html:1570 <button>
  i1570_click: function (event) { sharePopup(this) },
  // index.html:1578 <div>
  i1578_click: function (event) { closeWikiPopup() },
  // index.html:1588 <div id="chronam-popup-overlay">
  i1588_click: function (event) { if(event.target===this)closeChronAmPopup() },
  // index.html:1590 <div>
  i1590_click: function (event) { closeChronAmPopup() },
  // index.html:1598 <div id="loc-popup-overlay">
  i1598_click: function (event) { if(event.target===this)closeLocPopup() },
  // index.html:1600 <button>
  i1600_click: function (event) { closeLocPopup() },
  // index.html:1610 <div id="archive-popup-overlay">
  i1610_click: function (event) { if(event.target===this)closeArchivePopup() },
  // index.html:1612 <button>
  i1612_click: function (event) { closeArchivePopup() },
  // index.html:1621 <div id="youtube-popup-overlay">
  i1621_click: function (event) { if(event.target===this)_youtubePopupRequestClose() },
  // index.html:1623 <button>
  i1623_click: function (event) { _youtubePopupRequestClose() },
  // index.html:1630 <input id="youtube-popup-search-input">
  i1630_keydown: function (event) { if(event.key==='Enter'){event.preventDefault();_youtubeRerunSearch()} },
  // index.html:1632 <button>
  i1632_click: function (event) { _youtubeRerunSearch() },
  // index.html:1663 <button>
  i1663_click: function (event) { _youtubePastePresubmit(this) },
  // index.html:1676 <div id="barcode-scan-overlay">
  i1676_click: function (event) { if(event.target===this)window._sdCloseScanner&&window._sdCloseScanner() },
  // index.html:1678 <button>
  i1678_click: function (event) { window._sdCloseScanner&&window._sdCloseScanner() },
  // index.html:1690 <button id="barcode-scan-submit">
  i1690_click: function (event) { window._sdSubmitBarcodeManual&&window._sdSubmitBarcodeManual() },
  // index.html:1696 <div id="loc-info-overlay">
  i1696_click: function (event) { if(event.target===this)_locCloseInfoPopup() },
  // index.html:1698 <button>
  i1698_click: function (event) { _locCloseInfoPopup() },
  // index.html:1706 <div id="archive-info-overlay">
  i1706_click: function (event) { if(event.target===this)_archiveCloseInfoPopup() },
  // index.html:1708 <button>
  i1708_click: function (event) { _archiveCloseInfoPopup() },
  // index.html:1731 <button id="mini-player-expand-toggle">
  i1731_click: function (event) { event.stopPropagation();toggleMiniPlayer() },
  // index.html:1737 <button id="mini-share">
  i1737_click: function (event) { sharePlayerUrl() },
  // index.html:1738 <button id="mini-album">
  i1738_click: function (event) { openPlayerRelease(this) },
  // index.html:1740 <button id="mini-loc-save">
  i1740_click: function (event) { _locToggleSaveFromBar() },
  // index.html:1741 <button id="mini-loc-info">
  i1741_click: function (event) { _locOpenFromBar() },
  // index.html:1742 <button id="mini-prev">
  i1742_click: function (event) { playerPrev() },
  // index.html:1743 <button id="mini-playpause">
  i1743_click: function (event) { playerTogglePause() },
  // index.html:1746 <button id="mini-stop">
  i1746_click: function (event) { playerStop() },
  // index.html:1747 <button id="mini-next">
  i1747_click: function (event) { playerNext() },
  // index.html:1752 <button id="mini-queue">
  i1752_click: function (event) { queueToggleDrawer() },
  // index.html:1757 <button id="mini-hide">
  i1757_click: function (event) { hideMiniPlayerBar() },
  // index.html:1778 <button id="video-prev">
  i1778_click: function (event) { videoPrev() },
  // index.html:1779 <button>
  i1779_click: function (event) { _queueToggleShuffle() },
  // index.html:1780 <button>
  i1780_click: function (event) { toggleRepeat() },
  // index.html:1781 <button id="video-share">
  i1781_click: function (event) { sharePopup(this) },
  // index.html:1782 <button id="video-copy-url">
  i1782_click: function (event) { copyYoutubeUrl(this) },
  // index.html:1783 <button id="video-next">
  i1783_click: function (event) { playNextVideo() },
  // index.html:1794 <button id="mini-player-show-tab">
  i1794_click: function (event) { window._restorePlayerOrOpenQueue&&window._restorePlayerOrOpenQueue() },
  // index.html:1796 <div id="lightbox-overlay">
  i1796_click: function (event) { closeLightbox() },
  // index.html:1797 <button id="lightbox-close">
  i1797_click: function (event) { closeLightbox() },
  // index.html:1798 <button id="lightbox-prev">
  i1798_click: function (event) { lightboxStep(event,-1) },
  // index.html:1799 <img id="lightbox-img">
  i1799_click: function (event) { event.stopPropagation();lightboxStep(event,1) },
  // index.html:1800 <button id="lightbox-next">
  i1800_click: function (event) { lightboxStep(event,1) },
  // index.html:163
  i163_mouseover: function (event) { this.style.color='#ccc' },
  // index.html:406
  i406_mouseover: function (event) { this.style.color='#ccc' },
  // index.html:430
  i430_mouseover: function (event) { this.style.color='#ccc' },
  // index.html:517
  i517_keydown: function (event) { if(event.key==='Escape'){document.getElementById('wanted-q').value='';filterWantedItems()} },
  // index.html:519
  i519_mouseover: function (event) { this.style.color='#ccc' },
  // index.html:1260
  i1260_keydown: function (event) { _gutenbergFindOnKeyDown(event) },
  // index.html:1424
  i1424_input: function (event) { _bluesDbUpdateEditorLinks() },
  // index.html:163
  i163_mouseout: function (event) { this.style.color='#666' },
  // index.html:406
  i406_mouseout: function (event) { this.style.color='#666' },
  // index.html:430
  i430_mouseout: function (event) { this.style.color='#555' },
  // index.html:519
  i519_mouseout: function (event) { this.style.color='#555' },
  // admin-panel.html:17 <button id="admin-api-kill-btn">
  p17_click: function (event) { toggleApiKill() },
  // admin-panel.html:18 <button id="admin-revoke-btn">
  p18_click: function (event) { adminRevokeSessions() },
  // admin-panel.html:22 <button id="admin-view-btn">
  p22_click: function (event) { adminToggleViewAsUser() },
  // admin-panel.html:23 <button id="admin-feedback-btn">
  p23_click: function (event) { adminOpenFeedback() },
  // admin-panel.html:31 <button>
  p31_click: function (event) { switchAdminTab('overview') },
  // admin-panel.html:32 <button>
  p32_click: function (event) { switchAdminTab('media') },
  // admin-panel.html:33 <button>
  p33_click: function (event) { switchAdminTab('content') },
  // admin-panel.html:34 <button>
  p34_click: function (event) { switchAdminTab('system') },
  // admin-panel.html:35 <button>
  p35_click: function (event) { switchAdminTab('cache') },
  // admin-panel.html:36 <button>
  p36_click: function (event) { switchAdminTab('yt-review') },
  // admin-panel.html:37 <button>
  p37_click: function (event) { switchAdminTab('query') },
  // admin-panel.html:50 <button id="overview-kpis-refresh-btn">
  p50_click: function (event) { _adminClickRefresh(this, loadAdminOverview) },
  // admin-panel.html:64 <button id="media-stats-refresh-btn">
  p64_click: function (event) { _adminClickRefresh(this, loadAdminMediaStats) },
  // admin-panel.html:77 <button id="users-unified-refresh-btn">
  p77_click: function (event) { _adminClickRefresh(this, loadAdminUsersUnified) },
  // admin-panel.html:79 <button id="suggestions-run-self-btn">
  p79_click: function (event) { adminSuggestionsRunSelf() },
  // admin-panel.html:80 <button id="suggestions-run-all-btn">
  p80_click: function (event) { adminSuggestionsRunAll() },
  // admin-panel.html:81 <button id="admin-sync-stop-btn">
  p81_click: function (event) { adminSyncStop(this) },
  // admin-panel.html:86 <input id="users-unified-filter">
  p86_input: function (event) { _adminUnifiedFilterInput(this.value) },
  // admin-panel.html:96 <button>
  p96_click: function (event) { adminOpenApis() },
  // admin-panel.html:97 <button>
  p97_click: function (event) { loadAdminSystem() },
  // admin-panel.html:100 <details id="admin-audit-wrap">
  p100_toggle: function (event) { if(this.open)loadAdminAudit() },
  // admin-panel.html:110 <button id="db-stats-refresh-btn">
  p110_click: function (event) { loadDbStats(this) },
  // admin-panel.html:132 <button id="cw-refresh-btn">
  p132_click: function (event) { _adminClickRefresh(this, loadCacheWarm) },
  // admin-panel.html:143 <button>
  p143_click: function (event) { loadCacheRate() },
  // admin-panel.html:154 <button>
  p154_click: function (event) { loadRedundantPreview(this) },
  // admin-panel.html:164 <button>
  p164_click: function (event) { pruneRedundant(this) },
  // admin-panel.html:193 <select id="rcx-genre">
  p193_change: function (event) { _rcxSyncStyleList() },
  // admin-panel.html:225 <button id="rcx-labels-btn">
  p225_click: function (event) { _rcxToggleLabelsPanel(event) },
  // admin-panel.html:227 <input id="rcx-labels-search">
  p227_input: function (event) { _rcxRenderLabelsList() },
  // admin-panel.html:229 <button>
  p229_click: function (event) { _rcxClearLabels() },
  // admin-panel.html:253 <button>
  p253_click: function (event) { rcxPreview() },
  // admin-panel.html:257 <button>
  p257_click: function (event) { rcxDownload() },
  // admin-panel.html:258 <button>
  p258_click: function (event) { rcxDumpV1() },
  // admin-panel.html:260 <button>
  p260_click: function (event) { rcxDumpAll() },
  // admin-panel.html:262 <button>
  p262_click: function (event) { rcxDelete() },
  // admin-panel.html:287 <button>
  p287_click: function (event) { baxPreview() },
  // admin-panel.html:291 <button>
  p291_click: function (event) { baxDownload() },
  // admin-panel.html:306 <button id="ytr-refresh-btn">
  p306_click: function (event) { _adminClickRefresh(this, loadYtReview) },
  // admin-panel.html:312 <button id="ytr-coverage-btn">
  p312_click: function (event) { ytrLoadCoverage(this) },
  // admin-panel.html:317 <input id="ytr-daily-toggle">
  p317_change: function (event) { ytrToggleDaily(this.checked) },
  // admin-panel.html:321 <input id="ytr-auto-toggle">
  p321_change: function (event) { ytrToggleAuto(this.checked) },
  // admin-panel.html:324 <button id="ytr-apply-trust-btn">
  p324_click: function (event) { ytrApplyTrust() },
  // admin-panel.html:326 <input id="ytr-filter">
  p326_input: function (event) { ytrFilterInput(this.value) },
  // admin-panel.html:328 <details id="ytr-query-wrap">
  p328_toggle: function (event) { if(this.open)ytrLoadQueryConfig() },
  // admin-panel.html:335 <input id="ytr-q-album">
  p335_input: function (event) { ytrQueryPreview() },
  // admin-panel.html:337 <input id="ytr-q-track">
  p337_input: function (event) { ytrQueryPreview() },
  // admin-panel.html:339 <textarea id="ytr-q-noise">
  p339_input: function (event) { ytrQueryPreview() },
  // admin-panel.html:342 <input id="ytr-q-emb">
  p342_change: function (event) { ytrQueryPreview() },
  // admin-panel.html:353 <input id="ytr-qt-artist">
  p353_input: function (event) { ytrQueryPreview() },
  // admin-panel.html:354 <input id="ytr-qt-album">
  p354_input: function (event) { ytrQueryPreview() },
  // admin-panel.html:355 <input id="ytr-qt-track">
  p355_input: function (event) { ytrQueryPreview() },
  // admin-panel.html:359 <button>
  p359_click: function (event) { ytrSaveQueryConfig(this) },
  // admin-panel.html:360 <button>
  p360_click: function (event) { ytrResetQueryConfig(this) },
  // admin-panel.html:366 <button>
  p366_click: function (event) { ytrTestQuery(this) },
  // admin-panel.html:371 <details id="ytr-channels-wrap">
  p371_toggle: function (event) { if(this.open)loadYtChannels() },
  // admin-panel.html:378 <details id="ytr-bans-wrap">
  p378_toggle: function (event) { if(this.open)loadYtBans() },
  // admin-panel.html:384 <input id="ytr-ban-input">
  p384_keydown: function (event) { if(event.key==='Enter'){event.preventDefault();ytrBanFromInput();} },
  // admin-panel.html:385 <button>
  p385_click: function (event) { ytrBanFromInput() },
  // admin-panel.html:409 <button id="query-run-btn">
  p409_click: function (event) { queryRun() },
  // admin-panel.html:413 <button id="query-csv-btn">
  p413_click: function (event) { queryDownloadCsv() },
  // admin-panel.html:414 <button>
  p414_click: function (event) { document.getElementById('query-sql').value='';document.getElementById('query-results').innerHTML=''; },
  // admin-panel.html:449 <button id="api-log-refresh-btn">
  p449_click: function (event) { _adminClickRefresh(this, loadApiLog) },
  // admin-panel.html:454 <select id="api-log-filter">
  p454_change: function (event) { loadApiLog() },
  // admin-panel.html:458 <select id="api-log-hours">
  p458_change: function (event) { loadApiLog() },
  // admin-panel.html:462 <input id="api-log-errors-only">
  p462_change: function (event) { loadApiLog() },
  // admin-panel.html:463 <input id="api-log-scheduled-only">
  p463_change: function (event) { loadApiLog() },
  // admin-panel.html:481 <button id="submissions-refresh-btn">
  p481_click: function (event) { _adminClickRefresh(this, loadAdminSubmissions) },
  // admin-panel.html:483 <input id="submissions-filter">
  p483_input: function (event) { _filterAdminSubmissions(this) },
  // admin-panel.html:493 <button id="unavailable-refresh-btn">
  p493_click: function (event) { _adminClickRefresh(this, loadAdminUnavailable) },
  // admin-panel.html:495 <input id="unavailable-filter">
  p495_input: function (event) { _filterAdminUnavailable(this) },
  // admin-panel.html:506 <div id="admin-feedback-overlay">
  p506_click: function (event) { if(event.target===this)adminCloseFeedback() },
  // admin-panel.html:511 <button>
  p511_click: function (event) { adminCloseFeedback() },
  // admin-panel.html:520 <div id="admin-apis-overlay">
  p520_click: function (event) { if(event.target===this)adminCloseApis() },
  // admin-panel.html:524 <button id="api-health-refresh-btn">
  p524_click: function (event) { _adminClickRefresh(this, loadApiHealth) },
  // admin-panel.html:526 <select id="api-health-hours">
  p526_change: function (event) { loadApiHealth() },
  // admin-panel.html:530 <button>
  p530_click: function (event) { adminCloseApis() },
  // admin-panel.html:537 <div id="admin-items-overlay">
  p537_click: function (event) { if(event.target===this)closeAdminItems() },
  // admin-panel.html:545 <button id="admin-items-tab-col">
  p545_click: function (event) { adminItemsTab('collection') },
  // admin-panel.html:546 <button id="admin-items-tab-want">
  p546_click: function (event) { adminItemsTab('wantlist') },
  // admin-panel.html:547 <button id="admin-items-tab-fav">
  p547_click: function (event) { adminItemsTab('favorites') },
  // admin-panel.html:548 <button>
  p548_click: function (event) { closeAdminItems() },
  // admin.html:46 <button id="modal-close">
  a46_click: function (event) { closeModal() },
  // admin.html:47 <button>
  a47_click: function (event) { sharePopup(this) },
  // admin.html:54 <button id="bio-full-close">
  a54_click: function (event) { closeBioFull() },
  // admin.html:59 <div id="lightbox-overlay">
  a59_click: function (event) { closeLightbox() },
  // admin.html:60 <button id="lightbox-close">
  a60_click: function (event) { closeLightbox() },
  // admin.html:61 <button id="lightbox-prev">
  a61_click: function (event) { lightboxStep(event,-1) },
  // admin.html:63 <button id="lightbox-next">
  a63_click: function (event) { lightboxStep(event,1) },
  // admin-panel.html:405 <textarea id="query-sql">
  p405_keydown: function (event) { if((event.ctrlKey||event.metaKey)&&event.key==='Enter'){event.preventDefault();queryRun();} },
});
