document.addEventListener('DOMContentLoaded', async function() {
    // Presets aus JSON laden
    let presets = [];
    try {
        const response = await fetch('presets.json');
        presets = await response.json();
    } catch (e) {
        console.error('Could not load presets.json', e);
    }

    // Buttons dynamisch erzeugen
    const container = document.getElementById('presetContainer');
    if (container && presets.length) {
        container.innerHTML = '';
        presets.forEach(preset => {
            const btn = document.createElement('button');
            btn.type = 'button';
            btn.id = preset.id;
            btn.textContent = preset.label;
            btn.addEventListener('click', () => {
                fillFormFromPreset(preset.values);
            });
            container.appendChild(btn);
        });
    }

    function fillFormFromPreset(values) {
        // 1. Mode auf READ erzwingen (sicherheitshalber)
        const readRadio = document.querySelector('input[name="mode"][value="read"]');
        if (readRadio) readRadio.checked = true;

        // 2. Alle Filter- und Meta-Felder zurücksetzen
        const allInputs = document.querySelectorAll('#mainForm input, #mainForm select');
        allInputs.forEach(input => {
            if (input.type !== 'submit' && input.type !== 'button' && input.name) {
                if (input.tagName === 'SELECT') {
                    // Select nicht zurücksetzen
                } else {
                    input.value = '';
                }
            }
        });

        // 3. Werte aus dem Preset setzen
        for (const [key, val] of Object.entries(values)) {
            // Direktes Input-Feld
            let input = document.querySelector(`input[name="${key}"]`);
            if (input) {
                input.value = val;
                continue;
            }
            // Datumsgruppe
            const group = Array.from(document.querySelectorAll('[data-datefield]')).find(g => {
                const select = g.querySelector('.date-select');
                return select && Array.from(select.options).some(opt => opt.value === key);
            });
            if (group) {
                const select = group.querySelector('.date-select');
                const inputField = group.querySelector('.date-input');
                if (select && inputField) {
                    select.value = key;
                    inputField.name = key;
                    inputField.value = val;
                    select.dispatchEvent(new Event('change'));
                }
            }
        }

        // 4. Action auf 'execute' setzen
        const form = document.getElementById('mainForm');
        let actionInput = form.querySelector('input[name="action"]');
        if (!actionInput) {
            actionInput = document.createElement('input');
            actionInput.type = 'hidden';
            actionInput.name = 'action';
            form.appendChild(actionInput);
        }
        actionInput.value = 'execute';

        // 5. Nochmal sicherstellen, dass Mode wirklich 'read' ist (manche Browser speichern Zustand)
        if (readRadio) readRadio.checked = true;

        // 6. Absenden
        setTimeout(() => {
            form.submit();
        }, 200);

    }

    // Initialisierung der Datums-Selectoren (unverändert)
    function initDateSelectors() {
        document.querySelectorAll('[data-datefield]').forEach(container => {
            const input = container.querySelector('.date-input');
            const select = container.querySelector('.date-select');
            if (!input || !select) return;
            select.addEventListener('change', () => {
                const selectedField = select.value;
                input.name = selectedField;
            });
            select.dispatchEvent(new Event('change'));
        });
    }

    // Fallback: falls kein Radio ausgewählt, setze read
    const readRadio = document.querySelector('input[name="mode"][value="read"]');
    const writeRadio = document.querySelector('input[name="mode"][value="write"]');
    if ((!readRadio.checked && !writeRadio.checked) && readRadio) {
        readRadio.checked = true;
    }

    initDateSelectors();
});