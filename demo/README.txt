/*
 *       ██╗    ██╗ █████╗ ██╗   ██╗███████╗ ██████╗ ██╗
 *       ██║    ██║██╔══██╗██║   ██║██╔════╝██╔═══██╗██║
 *       ██║ █╗ ██║███████║██║   ██║█████╗  ██║   ██║██║
 *       ██║███╗██║██╔══██║╚██╗ ██╔╝██╔══╝  ██║▄▄ ██║██║
 *       ╚███╔███╔╝██║  ██║ ╚████╔╝ ███████╗╚██████╔╝███████╗
 *        ╚══╝╚══╝ ╚═╝  ╚═╝  ╚═══╝  ╚══════╝ ╚══▀▀═╝ ╚══════╝
 *
 *                         W A V E  Q L
 *                         ~~~~~~~~~~~~
 *                           by e2see
 *
 *
 */
╔══════════════════════════════════════════════════════════════════╗
║                     waveQl Test Environment                      ║
║                  SQL Query Builder – Live Demo                   ║
╚══════════════════════════════════════════════════════════════════╝

────────────────────────────────────────────────────────────────────
  QUICK START (RECOMMENDED)
────────────────────────────────────────────────────────────────────

▶ 1. Place the waveQl files
   Copy all required files into the parent directory in a "src" folder.

   Required files:
   • waveQl.php
   • waveQlCore.php
   • waveQlRead.php
   • waveQlWrite.php
   • waveQlException.php
   • waveQlDbInterface.php
   • dbAdapterMysqli.php

▶ 2. Configure the database
   Edit config.php – enter your MySQL credentials (host, user, password, dbname).
   The database will be created automatically if it doesn't exist.

▶ 3. Start the demo
   Open index.php in your browser.

   • If the database or tables are missing → a setup screen appears.
   • Click the button to auto‑initialize the database (runs setup.sql).
   • After success, the full demo UI loads.

▶ 4. Explore & test
   • Use the form to run SELECT queries with operators like >10, ~text~, etc.
   • Try the preset filters (e.g., "Large population", "Summer founders").
   • Add new countries (Write mode – INSERT).

────────────────────────────────────────────────────────────────────
  RESET THE DATABASE
────────────────────────────────────────────────────────────────────

   Simply open:   index.php?initSQL=1
   (or use the "reset/initialise database" link in the demo UI)

────────────────────────────────────────────────────────────────────
  HOW waveQl WORKS
────────────────────────────────────────────────────────────────────

  📖 READING (SELECT)
     Provide an array of filter values, e.g.:
        ['Population' => '>1000000']
     plus optional metadata (sort, pageSize, searchString, searchTarget).
     waveQl generates a secure SQL query and returns the result.

  ✍️ WRITING (INSERT / UPDATE / DELETE)
     Set meta with uniqueKey, e.g. setMeta(['uniqueKey' => 'id']).
     Then setValues([...]).
       • uniqueKey present in values → UPDATE
       • uniqueKey missing           → INSERT
     Option 'returning' gives back the full record.

  🎛️ OPERATORS & MAGIC KEYS
     > , < , ! , ~ , NULL , BLANK , EMPTY
     Ranges:   10><20 , 10><=20 , =><20 , =><=20
     All work as documented – directly inside the value string.

────────────────────────────────────────────────────────────────────
  NAMESPACE
────────────────────────────────────────────────────────────────────

   The test form uses the factory class:   \e2\waveQl

────────────────────────────────────────────────────────────────────
  ENJOY FILTERING! 💙
────────────────────────────────────────────────────────────────────