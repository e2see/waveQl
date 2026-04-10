<!DOCTYPE html>
<html>

<head>
    <meta charset="utf-8">
    <title>waveQl – Setup</title>
    <link rel="icon" type="image/png" href="../images/logo-xs.png" />
    <link rel="stylesheet" href="sub/core.css">
    <link rel="stylesheet" href="sub/template.css">
    <link rel="stylesheet" href="sub/components.css">
    <link rel="stylesheet" href="sub/desktop.css">
</head>

<body>
    <a class="logo-container" href="./">
        <img src="../images/logo-m.png" alt="waveQl Logo" id="logo" />
    </a>
    <h1>waveQl Playground – Setup</h1>

    <div class="message <?= strpos($setupMessage, '❌') !== false ? 'error' : 'warning' ?>">
        <?= $setupMessage ?>
    </div>

   <?php
    if (!$connectionOk) {
        echo '<div class="config-details">
                <p>Current configuration (<code>config.php</code>):</p>
                <ul>
                    <li>Host: <strong>' . htmlspecialchars($maskedHost) . '</strong></li>
                    <li>User: <strong>' . htmlspecialchars($maskedUser) . '</strong></li>
                    <li>Database: <strong>' . htmlspecialchars($maskedDb) . '</strong></li>
                </ul>
                <p>Please correct the credentials in <code>config.php</code>.</p>
            </div>';
    } elseif (!$tableExists) {
        echo '<div class="config-details">
                <p>✅ Connection to database <strong>' . htmlspecialchars($maskedDb) . '</strong> successful.</p>
                <p>⚠️ However, the table <code>countries</code> is missing.</p>
                <p>You can initialise the database automatically – the SQL script from <code>demo/setup.sql</code> will be executed.</p>';

        if ($allowInitSQL) {
            echo '<form method="get" action="">
                    <input type="hidden" name="initSQL" value="1">
                    <button type="submit" style="background: #2ecc71; color: white; border: none; padding: 0.6rem 1.2rem; font-weight: bold;">🚀 Run setup.sql now</button>
                </form>
                <p style="margin-top: 1rem;"><small>Alternatively, you can execute the file <code>demo/setup.sql</code> manually in your database tool.</small></p>';
        } else {
            echo '<p>❌ Automatic initialisation is disabled in the configuration (<code>$allowInitSQL = false;</code>).</p>
                <p>Please run <code>demo/setup.sql</code> manually in your database tool (e.g. phpMyAdmin, Adminer, or MySQL command line).</p>';
        }

        echo '</div>';
    }

    // ============================================================
    // initMessage anzeigen, falls vorhanden
    // ============================================================
    if (!empty($initMessage)) {
        echo '<div class="message error">' . htmlspecialchars($initMessage) . '</div>';
    }
    ?>
</body>

</html>