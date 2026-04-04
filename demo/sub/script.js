document.addEventListener('DOMContentLoaded', async function() {
    // Presets aus JSON laden (sub/presets.json)
    let presets = [];
    try {
        const response = await fetch('sub/presets.json');
        presets = await response.json();
    } catch (e) {
        console.error('Could not load presets.json', e);
    }

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
        const readRadio = document.querySelector('input[name="mode"][value="read"]');
        if (readRadio) readRadio.checked = true;

        const allInputs = document.querySelectorAll('#mainForm input, #mainForm select');
        allInputs.forEach(input => {
            if (input.type !== 'submit' && input.type !== 'button' && input.name) {
                if (input.tagName === 'SELECT') {
                    // nichts tun
                } else {
                    input.value = '';
                }
            }
        });

        for (const [key, val] of Object.entries(values)) {
            let input = document.querySelector(`input[name="${key}"]`);
            if (input) {
                input.value = val;
                continue;
            }
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

        const form = document.getElementById('mainForm');
        let actionInput = form.querySelector('input[name="action"]');
        if (!actionInput) {
            actionInput = document.createElement('input');
            actionInput.type = 'hidden';
            actionInput.name = 'action';
            form.appendChild(actionInput);
        }
        actionInput.value = 'execute';

        if (readRadio) readRadio.checked = true;

        setTimeout(() => {
            form.submit();
        }, 200);
    }

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

    // Radio-Status basierend auf HTML checked-Attribut erzwingen (gegen Browser-Cache)
    function enforceRadioState() {
        const radios = document.querySelectorAll('input[name="mode"]');
        let checkedRadio = null;
        for (const radio of radios) {
            if (radio.hasAttribute('checked')) {
                checkedRadio = radio;
                break;
            }
        }
        if (checkedRadio) {
            for (const radio of radios) {
                radio.checked = (radio === checkedRadio);
            }
        }
    }

    const readRadio = document.querySelector('input[name="mode"][value="read"]');
    const writeRadio = document.querySelector('input[name="mode"][value="write"]');
    if ((!readRadio.checked && !writeRadio.checked) && readRadio) {
        readRadio.checked = true;
    }

    enforceRadioState();   // zusätzliche Absicherung
    initDateSelectors();
});