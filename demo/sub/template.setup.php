<!DOCTYPE html>
<html>

<head>
    <meta charset="utf-8">
    <title>waveQl – Setup</title>
    <link rel="icon" type="image/png" href="../images/logo-xs.png" />
    <link rel="stylesheet" href="sub/style.css">
</head>

<body>
    <div class="logo-container">
        <img src="../images/logo-s.png" alt="waveQl Logo" id="logo" />
    </div>
    <h1>waveQl Test Environment – Setup</h1>

    <div class="message <?= strpos($setupMessage, '❌') !== false ? 'error' : 'warning' ?>">
        <?= $setupMessage ?>
    </div>

    <?php if (!$connectionOk): ?>
        <div class="config-details">
            <p>Current configuration (<code>config.php</code>):</p>
            <ul>
                <li>Host: <strong><?= htmlspecialchars($maskedHost) ?></strong></li>
                <li>User: <strong><?= htmlspecialchars($maskedUser) ?></strong></li>
                <li>Database: <strong><?= htmlspecialchars($maskedDb) ?></strong></li>
            </ul>
            <p>Please correct the credentials in <code>config.php</code>.</p>
        </div>
    <?php elseif (!$tableExists): ?>
        <div class="config-details">
            <p>Connection to database <strong><?= htmlspecialchars($maskedDb) ?></strong> successful.</p>
            <p>However, the <code>countries</code> table is missing.</p>
            <?php if ($allowInitSQL): ?>
                <form method="get" action="">
                    <input type="hidden" name="initSQL" value="1">
                    <button type="submit">📦 Automatically initialise database (run setup.sql)</button>
                </form>
            <?php else: ?>
                <p>Automatic initialisation is disabled in the configuration. Please run <code>setup.sql</code> manually.</p>
            <?php endif; ?>
        </div>
    <?php endif; ?>

    <?php if (!empty($initMessage)): ?>
        <div class="message error"><?= htmlspecialchars($initMessage) ?></div>
    <?php endif; ?>
</body>

</html>