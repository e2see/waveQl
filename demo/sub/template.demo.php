<!DOCTYPE html>
<html>
<head>
    <meta charset="utf-8">
    <title>waveQl Test Environment</title>
    <link rel="icon" type="image/png" href="../images/logo-xs.png" />
    <link rel="stylesheet" href="sub/style.css">
    <script src="sub/script.js" defer></script>
</head>
<body>
<div class="logo-container">
    <img src="../images/logo-s.png" alt="waveQl Logo" id="logo" />
</div>
<h1>waveQl Test Environment</h1>

<?php if ($allowInitSQL): ?>
    <p><a href="?initSQL=1">Click here to reset / initialise the database</a></p>
<?php else: ?>
    <p><span class="disabled-element" style="cursor: default;">Reset / initialise database (disabled in config)</span></p>
<?php endif; ?>

<div class="presets <?= !$allowRead ? 'disabled-element' : '' ?>" id="presetContainer"></div>

<?php if ($initMessage): ?>
    <div class="message <?= strpos($initMessage, 'successfully') !== false ? 'success' : 'error' ?>">
        <?= $initMessage ?>
    </div>
<?php endif; ?>

<?php if ($errorMsg): ?>
    <div class="message error"><?= $errorMsg ?></div>
<?php endif; ?>

<?php if (!$allowRead && !$allowWrite): ?>
    <div class="message error">Neither Read nor Write mode is allowed. Please check configuration.</div>
<?php elseif (!$allowRead): ?>
    <div class="message warning">Read mode is disabled. Only write operations allowed.</div>
<?php elseif (!$allowWrite): ?>
    <div class="message warning">Write mode is disabled. Only read operations allowed.</div>
<?php endif; ?>

<!-- Top row: three boxes -->
<div class="dashboard-row">
    <div class="dashboard-card">
        <div class="card-header">📋 Manifest (live)</div>
        <pre class="code-block">=== tableManifest ===
<?= json_encode($liveTableManifest, JSON_PRETTY_PRINT | JSON_UNESCAPED_SLASHES) ?>

=== keyManifest (liveOp) ===
<?= json_encode($liveKeyManifest, JSON_PRETTY_PRINT | JSON_UNESCAPED_SLASHES) ?>

=== metaManifest (live) ===
<?= json_encode($liveMetaManifest, JSON_PRETTY_PRINT | JSON_UNESCAPED_SLASHES) ?></pre>
    </div>
    <div class="dashboard-card">
        <div class="card-header">🔍 Current Filter</div>
        <pre class="code-block"><?= json_encode($filter, JSON_PRETTY_PRINT | JSON_UNESCAPED_SLASHES) ?></pre>
    </div>
    <div class="dashboard-card <?= $blinkSql ? 'blink-box' : '' ?>" id="sqlBox">
        <div class="card-header">📄 Generated SQL</div>
        <div class="sql-content"><?= $sqlOutput ?: '<em>No query generated yet.</em>' ?></div>
    </div>
</div>

<!-- Result area (full width) -->
<?php if ($resultOutput): ?>
    <div class="dashboard-card result-card <?= $blinkResult ? 'blink-box' : '' ?>" id="resultBox">
        <div class="card-header">📊 Result</div>
        <div class="result-content"><?= $resultOutput ?></div>
    </div>
<?php endif; ?>

<!-- Form for filters and controls -->
<form method="post" id="mainForm" autocomplete="off">
    <div class="dashboard-row">
        <!-- Left: Filters -->
        <div class="dashboard-card">
            <fieldset class="compact-fieldset">
                <legend>🎛️ Filters</legend>
                <div class="filter-header" style="display: flex; justify-content: space-between; margin-bottom: 0.5rem; font-weight: bold;">
                    <span style="width: 120px;">Field</span>
                    <span style="flex: 1;">Filter value</span>
                    <span style="width: auto;"><small>integrate into<br/>fulltextSearch</small></span>
                </div>
                <?php
                $defaultFulltextFields = ['CountryName', 'Capital', 'ContinentName'];
                $submittedFields = (array)($_POST['fulltext_fields'] ?? []);
                foreach ($originalFields as $field):
                    if (in_array($field, $dateFields)) continue; // Datumsfelder separat
                    $value = $_POST[$field] ?? '';
                    $placeholder = '';
                    $datalistId = '';
                    $datalistOptions = [];
                    switch ($field) {
                        case 'CountryName':
                            $placeholder = '~land~ or !NULL';
                            $datalistId = 'countryList';
                            $datalistOptions = $countryNames ?? [];
                            break;
                        case 'Population':
                            $placeholder = '>1000000 or 50000000><200000000';
                            $datalistId = 'populationExamples';
                            $datalistOptions = $populationExamples ?? [];
                            break;
                        case 'AreaKm2':
                            $placeholder = '>1000000 or 1000000><5000000';
                            $datalistId = 'areaExamples';
                            $datalistOptions = $areaExamples ?? [];
                            break;
                        case 'Capital':
                            $placeholder = '~Berlin~ or !BLANK';
                            $datalistId = 'capitalList';
                            $datalistOptions = $capitals ?? [];
                            break;
                        case 'FoundedYear':
                            $placeholder = '1800><1950 or >1900';
                            $datalistId = 'yearExamples';
                            $datalistOptions = $yearExamples ?? [];
                            break;
                        case 'ContinentName':
                            $placeholder = '~Asia~ or !EMPTY';
                            $datalistId = 'continentList';
                            $datalistOptions = $continentNames ?? [];
                            break;
                        default:
                            $placeholder = 'e.g. 5><9 or >3';
                            break;
                    }
                    if (empty($submittedFields) && in_array($field, $defaultFulltextFields)) {
                        $checked = true;
                    } else {
                        $checked = in_array($field, $submittedFields);
                    }
                ?>
                    <div class="form-row filter-row">
                        <label class="filter-label"><?= htmlspecialchars($field) ?>:</label>
                        <input type="text" name="<?= $field ?>" value="<?= htmlspecialchars($value) ?>" placeholder="<?= $placeholder ?>" list="<?= $datalistId ?>" class="filter-input">
                        <input type="checkbox" name="fulltext_fields[]" value="<?= htmlspecialchars($field) ?>" <?= $checked ? 'checked' : '' ?> class="filter-checkbox">
                        <?php if (!empty($datalistOptions)): ?>
                            <datalist id="<?= $datalistId ?>">
                                <?php foreach ($datalistOptions as $opt): ?>
                                    <option value="<?= htmlspecialchars($opt) ?>">
                                <?php endforeach; ?>
                            </datalist>
                        <?php endif; ?>
                    </div>
                <?php endforeach; ?>

                <!-- Date fields: separates Select für Funktion + Fulltext-Checkbox -->
                <?php foreach ($dateFields as $baseField): ?>
                    <?php
                    $inputValue = $_POST[$baseField] ?? '';
                    $selectedFunction = $_POST[$baseField . '_function'] ?? 'Original';
                    if (!$opt_virtualDateFields) {
                        $selectedFunction = 'Original';
                        $disabledAttr = 'disabled';
                    } else {
                        $disabledAttr = '';
                    }
                    $currentWaveQlKey = ($selectedFunction === 'Original') ? $baseField : $baseField . $selectedFunction;
                    $fulltextChecked = in_array($currentWaveQlKey, (array)($_POST['fulltext_fields'] ?? []));
                    ?>
                    <div class="form-row date-field-group" data-datefield="<?= $baseField ?>">
                        <label class="date-label"><?= htmlspecialchars($baseField) ?>:</label>
                        <input type="text" class="date-input" name="<?= $baseField ?>" value="<?= htmlspecialchars($inputValue) ?>" placeholder="e.g. 5><9 or >3">
                        <select class="date-select-right" name="<?= $baseField ?>_function" <?= $disabledAttr ?>>
                            <option value="Original" <?= $selectedFunction === 'Original' ? 'selected' : '' ?>>as Original</option>
                            <?php foreach ($virtualSuffixes as $suf): ?>
                                <option value="<?= $suf ?>" <?= $selectedFunction === $suf ? 'selected' : '' ?>>as <?= $suf ?></option>
                            <?php endforeach; ?>
                        </select>
                        <input type="checkbox" class="date-fulltext-checkbox" name="fulltext_fields[]" value="<?= htmlspecialchars($currentWaveQlKey) ?>" <?= $fulltextChecked ? 'checked' : '' ?>>
                    </div>
                <?php endforeach; ?>

                <!-- Fulltext search string -->
                <div class="form-row fulltext-row">
                    <label class="fulltext-label">fulltextSearch:</label>
                    <input type="text" name="fulltext_search_string" value="<?= htmlspecialchars($_POST['fulltext_search_string'] ?? '') ?>" placeholder="Enter search term..." class="fulltext-input">
                </div>
            </fieldset>
        </div>

        <!-- Middle: Meta -->
        <div class="dashboard-card">
            <fieldset class="compact-fieldset">
                <legend>⚙️ Meta (sorting, pagination)</legend>
                <div class="form-row">
                    <label class="meta-label">Sort:</label>
                    <input type="text" name="sort" value="<?= htmlspecialchars($_POST['sort'] ?? '') ?>" placeholder=">Population,<CountryName" list="sortExamples" class="meta-input">
                    <datalist id="sortExamples">
                        <?php foreach ($sortExamples as $opt): ?>
                            <option value="<?= htmlspecialchars($opt) ?>">
                        <?php endforeach; ?>
                    </datalist>
                </div>
                <div class="form-row">
                    <label class="meta-label">Page size:</label>
                    <input type="number" name="pageSize" value="<?= htmlspecialchars($_POST['pageSize'] ?? '') ?>" placeholder="20" class="meta-input">
                </div>
                <div class="form-row">
                    <label class="meta-label">Page number:</label>
                    <input type="number" name="pageNumber" value="<?= htmlspecialchars($_POST['pageNumber'] ?? '') ?>" placeholder="1" class="meta-input">
                </div>
                <?php if ($opt_allowSqlCondition): ?>
                    <div class="form-row">
                        <label class="meta-label">Custom SQL condition:</label>
                        <input type="text" name="sqlCondition" value="<?= htmlspecialchars($_POST['sqlCondition'] ?? '') ?>" placeholder="e.g. Population > 100000 AND Population < 120000000" class="meta-input">
                    </div>
                <?php endif; ?>
            </fieldset>
        </div>

        <!-- Right: Control -->
        <div class="dashboard-card">
            <fieldset class="compact-fieldset">
                <legend>🎮 Control</legend>
                <div class="form-row">
                    <label class="meta-label">Mode:</label>
                    <div class="radio-group">
                        <label class="radio-label">
                            <input type="radio" name="mode" value="read" <?= $mode === 'read' ? 'checked' : '' ?> <?= !$allowRead ? 'disabled' : '' ?>>
                            Read (SELECT)
                        </label>
                        <label class="radio-label">
                            <input type="radio" name="mode" value="write" <?= $mode === 'write' ? 'checked' : '' ?> <?= !$allowWrite ? 'disabled' : '' ?>>
                            Write (INSERT)
                        </label>
                    </div>
                </div>
                <div class="form-row">
                    <label class="meta-label">Actions:</label>
                    <div>
                        <?php $actionDisabled = (!$allowRead && !$allowWrite) ? 'disabled' : ''; ?>
                        <button type="submit" name="action" value="preview" <?= $actionDisabled ?> class="<?= $actionDisabled ? 'disabled-element' : '' ?>">SQL preview</button>
                        <button type="submit" name="action" value="execute" <?= $actionDisabled ?> class="<?= $actionDisabled ? 'disabled-element' : '' ?>">Execute</button>
                    </div>
                </div>
                <div class="form-row">
                    <label class="meta-label">Options:</label>
                    <div class="checkbox-group-vertical">
                        <label class="checkbox-label">
                            <input type="checkbox" name="opt_virtualDateFields" <?= !$opt_virtualDateFields ? 'checked' : '' ?>>
                            disable virtualDateFields
                        </label>
                        <label class="checkbox-label">
                            <input type="checkbox" name="opt_allowSqlCondition" <?= !$opt_allowSqlCondition ? 'checked' : '' ?>>
                            disable allowSqlCondition
                        </label>
                        <label class="checkbox-label">
                            <input type="checkbox" name="opt_prepared" <?= !$opt_prepared ? 'checked' : '' ?>>
                            disable prepared
                        </label>
                    </div>
                </div>
            </fieldset>
        </div>
    </div>

    <!-- Write mode specific fields -->
    <?php if ($mode === 'write'): ?>
    <div class="dashboard-row">
        <div class="dashboard-card">
            <fieldset class="compact-fieldset">
                <legend>✏️ Write data (INSERT)</legend>
                <div class="form-row">
                    <label class="meta-label">CountryName:</label>
                    <input type="text" name="CountryName" value="<?= htmlspecialchars($_POST['CountryName'] ?? '') ?>">
                </div>
                <div class="form-row">
                    <label class="meta-label">Population:</label>
                    <input type="text" name="Population" value="<?= htmlspecialchars($_POST['Population'] ?? '') ?>">
                </div>
                <div class="form-row">
                    <label class="meta-label">AreaKm2:</label>
                    <input type="text" name="AreaKm2" value="<?= htmlspecialchars($_POST['AreaKm2'] ?? '') ?>">
                </div>
                <div class="form-row">
                    <label class="meta-label">Capital:</label>
                    <input type="text" name="Capital" value="<?= htmlspecialchars($_POST['Capital'] ?? '') ?>">
                </div>
                <div class="form-row">
                    <label class="meta-label">FoundedYear:</label>
                    <input type="text" name="FoundedYear" value="<?= htmlspecialchars($_POST['FoundedYear'] ?? '') ?>">
                </div>
                <div class="form-row">
                    <label class="meta-label">Continent (ID):</label>
                    <input type="number" name="continent_id" value="<?= htmlspecialchars($_POST['continent_id'] ?? '') ?>">
                </div>
            </fieldset>
        </div>
    </div>
    <?php endif; ?>
</form>

</body>
</html>