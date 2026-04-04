<!DOCTYPE html>
<html>

<head>
    <meta charset="utf-8">
    <title>waveQl – Setup</title>
    <link rel="icon" type="image/png" href="../images/logo-xs.png" />
    <link rel="stylesheet" href="sub/style.css">
</head>

<body>
    <img src="../images/logo-s.png" alt="waveQl Logo" id="logo" />
    <h1>waveQl Test Environment – Setup</h1>

    <div class="message <?= strpos($setupMessage, '❌') !== false ? 'error' : 'warning' ?>">
        <?= $setupMessage ?>
    </div>

    <?php if (!$connectionOk): ?>
        <div class="config-details">
            <p>Aktuelle Konfiguration (<code>config.php</code>):</p>
            <ul>
                <li>Host: <strong><?= htmlspecialchars($maskedHost) ?></strong></li>
                <li>Benutzer: <strong><?= htmlspecialchars($maskedUser) ?></strong></li>
                <li>Datenbank: <strong><?= htmlspecialchars($maskedDb) ?></strong></li>
            </ul>
            <p>Bitte korrigieren Sie die Zugangsdaten in der <code>config.php</code>.</p>
        </div>
    <?php elseif (!$tableExists): ?>
        <div class="config-details">
            <p>Die Verbindung zur Datenbank <strong><?= htmlspecialchars($maskedDb) ?></strong> ist erfolgreich.</p>
            <p>Die Tabelle <code>countries</code> fehlt jedoch.</p>
            <form method="get" action="">
                <input type="hidden" name="initSQL" value="1">
                <button type="submit">📦 Datenbank automatisch initialisieren (setup.sql ausführen)</button>
            </form>
            <p>Oder führen Sie die <code>setup.sql</code> manuell aus.</p>
        </div>
    <?php endif; ?>

    <?php if (!empty($initMessage)): ?>
        <div class="message error"><?= htmlspecialchars($initMessage) ?></div>
    <?php endif; ?>
</body>

</html>