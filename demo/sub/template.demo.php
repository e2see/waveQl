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
    <img src="../images/logo-s.png" alt="waveQl Logo" id="logo" />
    <h1>waveQl Test Environment</h1>
    <p><a href="?initSQL=1">Click here to reset / initialise the database</a></p>

    <div class="presets" id="presetContainer"></div>

    <?php if ($initMessage): ?>
        <div class="message <?= strpos($initMessage, 'successfully') !== false ? 'success' : 'error' ?>">
            <?= $initMessage ?>
        </div>
    <?php endif; ?>

    <?php if ($errorMsg): ?>
        <div class="message error"><?= $errorMsg ?></div>
    <?php endif; ?>

    <div class="dashboard-row dash-result">
        <div class="dashboard-card">
            <div class="card-header">📋 Field Definitions</div>
            <pre class="code-block"><?= highlight_string("<?php\n" . print_r($keyManifest, true) . "\n?>", true) ?></pre>
        </div>
        <div class="dashboard-card">
            <div class="card-header">🔍 Current Filter</div>
            <pre class="code-block"><?= highlight_string("<?php\n" . print_r($filter, true) . "\n?>", true) ?></pre>
        </div>
        <div class="dashboard-card">
            <div class="card-header">📄 Generated SQL</div>
            <div class="sql-content"><?= $sqlOutput ?: '<em>No query generated yet.</em>' ?></div>
        </div>
    </div>

    <?php if ($resultOutput): ?>
        <div class="result-box">
            <fieldset class="output-fieldset">
                <legend>📊 Result</legend>
                <div class="result-content"><?= $resultOutput ?></div>
            </fieldset>
        </div>
    <?php endif; ?>

    <form method="post" id="mainForm" autocomplete="off">
        <div class="dashboard-row">
            <div class="dashboard-card">
                <fieldset class="compact-fieldset">
                    <legend>Filters</legend>
                    <?php
                    // Gruppiere Felder: Basis-Datumsfelder vs. normale Felder
                    $normalFields = [];
                    $dateFieldGroups = [];
                    if (!isset($dateFields)) $dateFields = [];
                    foreach ($dateFields as $baseField) {
                        $autoFields = [];
                        foreach ($filterFields as $f) {
                            if (strpos($f, $baseField) === 0 && $f !== $baseField) {
                                $autoFields[] = $f;
                            }
                        }
                        $dateFieldGroups[$baseField] = $autoFields;
                    }
                    $allDateRelated = array_merge($dateFields, ...array_values($dateFieldGroups));
                    $normalFields = array_diff($filterFields, $allDateRelated);

                    foreach ($normalFields as $field):
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
                                $placeholder = 'z.B. 5><9 oder >3';
                                break;
                        }
                    ?>
                        <label><?= htmlspecialchars($field) ?>:</label>
                        <input type="text" name="<?= $field ?>" value="<?= htmlspecialchars($value) ?>" placeholder="<?= $placeholder ?>" list="<?= $datalistId ?>">
                        <?php if (!empty($datalistOptions)): ?>
                            <datalist id="<?= $datalistId ?>">
                                <?php foreach ($datalistOptions as $opt): ?>
                                    <option value="<?= htmlspecialchars($opt) ?>">
                                    <?php endforeach; ?>
                            </datalist>
                        <?php endif; ?>
                    <?php endforeach; ?>

                    <!-- Datumsfelder mit Selectbox -->
                    <?php foreach ($dateFieldGroups as $baseField => $autoFields): ?>
                        <?php
                        $currentField = $_POST[$baseField . '_selector'] ?? $baseField;
                        $inputValue = $_POST[$currentField] ?? '';
                        $placeholder = 'z.B. 5><9 oder >3';
                        ?>
                        <div class="date-field-group" data-datefield="<?= $baseField ?>">
                            <label><?= htmlspecialchars($baseField) ?>:</label>
                            <div style="display: flex; gap: 0.5rem; align-items: center;">
                                <select class="date-select" name="<?= $baseField ?>_selector" style="width: auto;">
                                    <option value="<?= $baseField ?>" <?= $currentField === $baseField ? 'selected' : '' ?>>[Original] <?= $baseField ?></option>
                                    <?php foreach ($autoFields as $auto): ?>
                                        <option value="<?= $auto ?>" <?= $currentField === $auto ? 'selected' : '' ?>><?= $auto ?></option>
                                    <?php endforeach; ?>
                                </select>
                                <input type="text" class="date-input" name="<?= $currentField ?>" value="<?= htmlspecialchars($inputValue) ?>" placeholder="<?= $placeholder ?>" style="flex: 1;">
                            </div>
                        </div>
                    <?php endforeach; ?>
                </fieldset>
            </div>

            <div class="dashboard-card">
                <fieldset class="compact-fieldset">
                    <legend>Meta (sorting, pagination, search)</legend>
                    <label>Sort:</label>
                    <input type="text" name="sort" value="<?= htmlspecialchars($_POST['sort'] ?? '') ?>" placeholder=">Population,<CountryName" list="sortExamples">
                    <datalist id="sortExamples">
                        <?php foreach ($sortExamples as $opt): ?>
                            <option value="<?= htmlspecialchars($opt) ?>">
                            <?php endforeach; ?>
                    </datalist>
                    <label>Page size:</label>
                    <input type="number" name="pageSize" value="<?= htmlspecialchars($_POST['pageSize'] ?? '') ?>" placeholder="20">
                    <label>Page number:</label>
                    <input type="number" name="pageNumber" value="<?= htmlspecialchars($_POST['pageNumber'] ?? '') ?>" placeholder="1">
                    <label>Search string:</label>
                    <input type="text" name="searchString" value="<?= htmlspecialchars($_POST['searchString'] ?? '') ?>" placeholder="e.g. 'land'">
                    <label>Search in fields:</label>
                    <input type="text" name="searchTarget" value="<?= htmlspecialchars($_POST['searchTarget'] ?? '') ?>" placeholder="CountryName,Capital">
                </fieldset>
            </div>

            <div class="dashboard-card">
                <fieldset class="compact-fieldset">
                    <legend>Control</legend>
                    <div class="control-group">
                        <label>Mode:</label>
                        <label class="radio-label">
                            <input type="radio" name="mode" value="read" <?= $mode === 'read' ? 'checked' : '' ?>> Read (SELECT)
                        </label>
                        <label class="radio-label">
                            <input type="radio" name="mode" value="write" <?= $mode === 'write' ? 'checked' : '' ?>> Write (INSERT)
                        </label>
                    </div>
                    <div class="control-group">
                        <button type="submit" name="action" value="query">Show SQL only</button>
                        <button type="submit" name="action" value="execute">Show SQL and execute</button>
                    </div>
                </fieldset>
            </div>
        </div>
    </form>
</body>

</html>