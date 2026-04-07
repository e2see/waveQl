<!DOCTYPE html>
<html>

<head>
    <meta charset="utf-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0, user-scalable=yes">
    <title>waveQl Playground </title>
    <link rel="icon" type="image/png" href="../images/logo-xs.png" />
    <link rel="stylesheet" href="sub/core.css">
    <link rel="stylesheet" href="sub/template.css">
    <link rel="stylesheet" href="sub/components.css">
    <link rel="stylesheet" href="sub/desktop.css">
    <script>
        var dateFieldNames        = <?= json_encode($dateFields) ?>;
        var virtualSuffixes       = <?= json_encode($allVirtualSuffixes) ?>;
        var foreignKeyField       = <?= json_encode($foreignKeyLogicalName) ?>;
        var allFields             = <?= json_encode($originalFields) ?>;
        var writeFieldNames       = <?= json_encode($writeFieldNames) ?>;
        var defaultFulltextFields = <?= json_encode($defaultFulltextFields) ?>;
    </script>
    <script src="sub/script.js" defer></script>
</head>

<body>
    <a class="logo-container" href="./">
        <img src="../images/logo-s.png" alt="waveQl Logo" id="logo" />
    </a>
    <h1>waveQl Playground</h1>

    <?php if ($allowInitSQL): ?>
        <p><a href="?initSQL=1">Click here to reset / initialise the database</a></p>
    <?php else: ?>
        <p><span class="disabled-element" style="cursor: default;">Reset / initialise database (disabled in config)</span></p>
    <?php endif; ?>

    <div class="presets-scroll-wrapper<?=(!$allowRead)? ' disabled-element': ''; ?>">
        <div class="presets-scroll" id="presetContainer"></div>
    </div>

    <?php if ($initMessage): ?>
        <div class="message <?= strpos($initMessage, 'successfully') !== false ? 'success' : 'error' ?>">
            <?= $initMessage ?>
        </div>
    <?php endif; ?>

    <?php if ($errorMsg): ?>
        <div class="message error"><?= $errorMsg ?></div>
    <?php endif; ?>

    <!-- Nur eine Fehlermeldung, wenn beide Modi deaktiviert sind -->
    <?php if ($modeError): ?>
        <div class="message error"><?= htmlspecialchars($modeError) ?></div>
    <?php endif; ?>

    <!-- Warnungen, wenn nur ein Modus deaktiviert ist (aber nicht beide) -->
    <?php if (!$modeError): ?>
        <?php if (!$allowRead && $allowWrite): ?>
            <div class="message warning">Read mode is disabled. Only write operations allowed.</div>
        <?php elseif ($allowRead && !$allowWrite): ?>
            <div class="message warning">Write mode is disabled. Only read operations allowed.</div>
        <?php endif; ?>
    <?php endif; ?>

    <?php if ($mode !== null): ?>
        <!-- Obere drei Boxen (immer sichtbar) -->
        <div class="dashboard-row">
            <div class="dashboard-card collapsible">
                <div class="card-header">📋 Manifest<button class="toggle-btn" type="button">▼</button></div>
                <div class="card-body">
                    <pre class="code-block">##### tableManifest<?=
                    htmlspecialchars(json_encode($tableManifest, JSON_PRETTY_PRINT | JSON_UNESCAPED_SLASHES)).PHP_EOL
                    ?>##### keyManifest<?=
                    htmlspecialchars(json_encode($keyManifest, JSON_PRETTY_PRINT | JSON_UNESCAPED_SLASHES)).PHP_EOL
                    ?></pre>
                </div>
            </div>
            <div class="dashboard-card collapsible">
                <div class="card-header">🔍 Current Filter <button class="toggle-btn" type="button">▼</button></div>
                <div class="card-body">
                    <pre class="code-block">##### setValues:<?=
                     htmlspecialchars(json_encode($filter, JSON_PRETTY_PRINT | JSON_UNESCAPED_SLASHES)).PHP_EOL
                    ?>##### setMeta:<?=
                    htmlspecialchars(json_encode($metaManifest, JSON_PRETTY_PRINT | JSON_UNESCAPED_SLASHES)).PHP_EOL
                    ?></pre>
                </div>
            </div>
            <div class="dashboard-card <?= $blinkSql ? 'blink-box' : '' ?>" id="sqlBox">
                <div class="card-header">📄 Generated SQL</div>
                <div class="card-body">
                    <div class="sql-content"><?= $sqlOutput ?: '<em>No query generated yet.</em>' ?></div>
                </div>
            </div>
        </div>

        <?php if ($resultOutput): ?>
            <div class="dashboard-card result-card <?= $blinkResult ? 'blink-box' : '' ?>" id="resultBox">
                <div class="card-header">📊 Result</div>
                <div class="card-body result-content"><?= $resultOutput ?></div>
            </div>
        <?php endif; ?>

        <form method="get" id="mainForm" autocomplete="off">
            <div class="dashboard-row">
                <!-- Filter-Box (immer sichtbar) -->
                <div class="dashboard-card">
                    <div class="card-header">🎛️ Filters / Write Input</div>
                    <div class="card-body">
                        <fieldset class="compact-fieldset">
                            <div class="filter-header" style="display: flex; justify-content: space-between; margin-bottom: 0.5rem; font-weight: bold;">
                                <span style="width: 120px;">Field</span><span style="flex: 1;">Value</span><span style="width: auto;text-align: right;"><small>integrate in<br/>Fulltext-Search</small></span>
                            </div>
                            <?php
                            $submittedFields = (array)($_GET['fulltext_fields'] ?? []);
                            $allFields       = $originalFields;

                            foreach ($allFields as $field):
                                if ($field === $foreignKeyLogicalName) continue;
                                if (in_array($field, $dateFields)) continue;
                                $value           = $_GET[$field] ?? '';
                                $placeholder     = '';
                                $datalistId      = '';
                                $datalistOptions = [];
                                switch ($field) {
                                    case 'CountryName':
                                        $placeholder     = '~land~ or !NULL';
                                        $datalistId      = 'countryList';
                                        $datalistOptions = $countryNames ?? [];
                                        break;
                                    case 'Population':
                                        $placeholder     = '>1000000 or 50000000><200000000';
                                        $datalistId      = 'populationExamples';
                                        $datalistOptions = $populationExamples ?? [];
                                        break;
                                    case 'AreaKm2':
                                        $placeholder     = '>1000000 or 1000000><5000000';
                                        $datalistId      = 'areaExamples';
                                        $datalistOptions = $areaExamples ?? [];
                                        break;
                                    case 'Capital':
                                        $placeholder     = '~Berlin~ or !BLANK';
                                        $datalistId      = 'capitalList';
                                        $datalistOptions = $capitals ?? [];
                                        break;
                                    case 'ContinentName':
                                        $placeholder     = '~Asia~ or !EMPTY';
                                        $datalistId      = 'continentList';
                                        $datalistOptions = $continentNames ?? [];
                                        break;
                                    default:
                                        $placeholder = 'e.g. 5><9f or >3';
                                        break;
                                }

                                $showCheckbox = !in_array($field, $dateFields);
                                $checked = false;
                                if ($showCheckbox) {
                                    if (empty($submittedFields) && in_array($field, $defaultFulltextFields)) $checked = true;
                                    else $checked = in_array($field, $submittedFields);
                                }
                            ?>
                                <div class="form-row filter-row">
                                    <label class="filter-label"><?= htmlspecialchars($field) ?>:</label>
                                    <input type="text" name="<?= $field ?>" value="<?= htmlspecialchars($value) ?>" placeholder="<?= $placeholder ?>" list="<?= $datalistId ?>" class="filter-input">
                                    <?php if ($showCheckbox): ?>
                                        <input type="checkbox" name="fulltext_fields[]" value="<?= htmlspecialchars($field) ?>" <?= $checked ? 'checked' : '' ?> class="filter-checkbox">
                                    <?php else: ?>
                                        <span style="width: 1.5rem;"></span>
                                    <?php endif; ?>
                                    <?php if (!empty($datalistOptions)): ?>
                                        <datalist id="<?= $datalistId ?>"><?php foreach ($datalistOptions as $opt): ?><option value="<?= htmlspecialchars($opt) ?>"><?php endforeach; ?></datalist>
                                    <?php endif; ?>
                                </div>
                            <?php endforeach; ?>

                            <!-- Fremdschlüssel (contentID) – nur im Write-Modus aktiv -->
                            <?php if ($foreignKeyLogicalName): ?>
                            <div class="form-row filter-row">
                                <label class="filter-label"><?= htmlspecialchars($foreignKeyLogicalName) ?>:</label>
                                <input type="text" name="<?= htmlspecialchars($foreignKeyLogicalName) ?>" <?= ($mode === 'read') ? 'disabled' : '' ?> value="<?= htmlspecialchars($_GET[$foreignKeyLogicalName] ?? '') ?>" placeholder="<?= htmlspecialchars($foreignKeyLogicalName) ?> ID (z.B. 1-7)" class="filter-input">
                                <span style="width: 1.5rem;"></span>
                            </div>
                            <?php endif; ?>

                            <?php foreach ($dateFields as $baseField): ?>
                                <?php
                                $inputValue       = $_GET[$baseField] ?? '';
                                $selectedFunction = $_GET[$baseField . '_function'] ?? 'Original';

                                if (!$opt_virtualDateFields) {
                                    $selectedFunction = 'Original';
                                    $disabledAttr     = 'disabled';
                                } else {
                                    $disabledAttr = '';
                                }

                                $currentWaveQlKey = ($selectedFunction === 'Original') ? $baseField : $baseField . $selectedFunction;
                                $fulltextChecked  = in_array($currentWaveQlKey, (array)($_GET['fulltext_fields'] ?? []));
                                $units            = $enrichedFields[$baseField]['units'] ?? [];

                                ?>
                                <div class="form-row date-field-group" data-datefield="<?= $baseField ?>">
                                    <label class="date-label"><?= htmlspecialchars($baseField) ?>:</label>
                                    <input type="text" class="date-input" name="<?= $baseField ?>" value="<?= htmlspecialchars($inputValue) ?>" placeholder="e.g. 1923-10-29 or 4 QUARTER">
                                    <select class="date-select-right" name="<?= $baseField ?>_function" <?= $disabledAttr ?>>
                                        <option value="Original" <?= $selectedFunction === 'Original' ? 'selected' : '' ?>>as Original</option>
                                        <?php foreach ($units as $suf): ?>
                                            <option value="<?= $suf ?>" <?= $selectedFunction === $suf ? 'selected' : '' ?>>as <?= $suf ?></option>
                                        <?php endforeach; ?>
                                    </select>
                                    <input type="checkbox" class="date-fulltext-checkbox" name="fulltext_fields[]" value="<?= htmlspecialchars($currentWaveQlKey) ?>" <?= $fulltextChecked ? 'checked' : '' ?>>
                                </div>
                            <?php endforeach; ?>
                        </fieldset>
                    </div>
                </div>

                <!-- Meta-Box -->
                <div class="dashboard-card">
                    <div class="card-header">⚙️ Meta (sorting, pagination)</div>
                    <div class="card-body">
                        <fieldset class="compact-fieldset">
                            <div class="form-row fulltext-row">
                                <label class="fulltext-label">Fulltext-Search:</label>
                                <input type="text" name="fulltext_search_string" value="<?= htmlspecialchars($_GET['fulltext_search_string'] ?? '') ?>" placeholder="Enter search term..." class="fulltext-input">
                            </div>
                            <div class="form-row"><label class="meta-label">Sort:</label><input type="text" name="sort" value="<?= htmlspecialchars($_GET['sort'] ?? '') ?>" placeholder=">Population,<CountryName" list="sortExamples" class="meta-input"><datalist id="sortExamples"><?php foreach ($sortExamples as $opt): ?><option value="<?= htmlspecialchars($opt) ?>"><?php endforeach; ?></datalist></div>
                            <div class="form-row"><label class="meta-label">Page size:</label><input type="number" name="pageSize" value="<?= htmlspecialchars($_GET['pageSize'] ?? '') ?>" placeholder="20" class="meta-input"></div>
                            <div class="form-row"><label class="meta-label">Page number:</label><input type="number" name="pageNumber" value="<?= htmlspecialchars($_GET['pageNumber'] ?? '') ?>" placeholder="1" class="meta-input"></div>
                            <?php if ($opt_allowSqlCondition): ?>
                                <div class="form-row"><label class="meta-label">Custom SQL condition:</label><input type="text" name="sqlCondition" value="<?= htmlspecialchars($_GET['sqlCondition'] ?? '') ?>" placeholder="e.g. Population > 100000 AND Population < 120000000" class="meta-input"></div>
                            <?php endif; ?>
                        </fieldset>
                    </div>
                </div>

                <!-- Control-Box -->
                <div class="dashboard-card">
                    <div class="card-header">🎮 Control</div>
                    <div class="card-body">
                        <fieldset class="compact-fieldset">
                            <div class="form-row"><label class="meta-label">Mode:</label>
                                <div class="radio-group"><label class="radio-label"><input type="radio" name="mode" value="read" <?= $mode === 'read' ? 'checked' : '' ?> <?= !$allowRead ? 'disabled' : '' ?>> Read (SELECT)</label><label class="radio-label"><input type="radio" name="mode" value="write" <?= $mode === 'write' ? 'checked' : '' ?> <?= !$allowWrite ? 'disabled' : '' ?>> Write (INSERT)</label></div>
                            </div>
                            <div class="form-row"><label class="meta-label">Options:</label>
                                <div class="checkbox-group-vertical"><label class="checkbox-label"><input type="checkbox" name="options[virtualDateFields]" <?= !$opt_virtualDateFields ? 'checked' : '' ?>> disable virtualDateFields</label><label class="checkbox-label"><input type="checkbox" name="options[allowSqlCondition]" <?= !$opt_allowSqlCondition ? 'checked' : '' ?>> disable allowSqlCondition</label><label class="checkbox-label"><input type="checkbox" name="options[prepared]" <?= !$opt_prepared ? 'checked' : '' ?>> disable prepared</label></div>
                            </div>
                        </fieldset>
                    </div>
                </div>
            </div>

            <!-- Fixierte Buttons -->
            <div class="action-buttons-fixed">
                <button type="submit" name="action" value="preview">🔍 SQL preview</button>
                <button type="submit" name="action" value="execute">⚡ Execute</button>
            </div>
        </form>
    <?php endif; ?>
</body>

</html>