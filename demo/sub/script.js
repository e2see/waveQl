document.addEventListener('DOMContentLoaded', async function () {
    const logo = document.getElementById('logo');
    if (logo) {
        let hideTimeout = null;
        function startHideTimer() {
            if (hideTimeout) clearTimeout(hideTimeout);
            hideTimeout = setTimeout(() => {
                logo.classList.remove('logo-hidden');
                hideTimeout = null;
            }, 2000);
        }
        logo.addEventListener('mouseenter', () => {
            logo.classList.add('logo-hidden');
            startHideTimer();
        });
    }

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
                if (input.type === 'checkbox') {
                    input.checked = false;
                } else {
                    input.value = '';
                }
            }
        });

        for (const [key, val] of Object.entries(values)) {
            if (key === 'searchTarget') {
                const fields = val.split(',').map(f => f.trim());
                const checkboxes = document.querySelectorAll('input[name="fulltext_fields[]"]');
                checkboxes.forEach(cb => {
                    if (fields.includes(cb.value)) {
                        cb.checked = true;
                    }
                });
            } else if (key === 'searchString') {
                const ftInput = document.querySelector('input[name="fulltext_search_string"]');
                if (ftInput) ftInput.value = val;
            } else {
                let matchedBase = null;
                let matchedSuffix = null;
                for (const base of dateFieldNames) {
                    if (key === base) {
                        matchedBase = base;
                        matchedSuffix = 'Original';
                        break;
                    }
                    if (key.startsWith(base)) {
                        const suffix = key.substring(base.length);
                        if (virtualSuffixes.includes(suffix)) {
                            matchedBase = base;
                            matchedSuffix = suffix;
                            break;
                        }
                    }
                }
                if (matchedBase) {
                    const group = document.querySelector(`.date-field-group[data-datefield="${matchedBase}"]`);
                    if (group) {
                        const textInput = group.querySelector('.date-input');
                        const select = group.querySelector('.date-select-right');
                        if (textInput) textInput.value = val;
                        if (select) {
                            for (let i = 0; i < select.options.length; i++) {
                                if (select.options[i].value === matchedSuffix) {
                                    select.selectedIndex = i;
                                    break;
                                }
                            }
                            select.dispatchEvent(new Event('change'));
                        }
                    }
                } else {
                    let input = document.querySelector(`input[name="${key}"]`);
                    if (input) {
                        input.value = val;
                    }
                }
            }
        }

        let actionInput = document.querySelector('input[name="action"]');
        if (!actionInput) {
            actionInput = document.createElement('input');
            actionInput.type = 'hidden';
            actionInput.name = 'action';
            document.getElementById('mainForm').appendChild(actionInput);
        }
        actionInput.value = 'execute';

        setTimeout(() => {
            document.getElementById('mainForm').submit();
        }, 200);
    }

    const blinkBoxes = document.querySelectorAll('.blink-box');
    blinkBoxes.forEach(box => {
        setTimeout(() => {
            box.classList.remove('blink-box');
        }, 3000);
    });

    const virtualCheckbox = document.querySelector('input[name="opt_virtualDateFields"]');
    const dateGroups = document.querySelectorAll('.date-field-group');

    function updateCheckboxValue(group) {
        const select = group.querySelector('.date-select-right');
        const checkbox = group.querySelector('.date-fulltext-checkbox');
        const baseField = group.getAttribute('data-datefield');
        if (!select || !checkbox || !baseField) return;
        const selectedValue = select.value;
        const waveQlKey = (selectedValue === 'Original') ? baseField : baseField + selectedValue;
        checkbox.value = waveQlKey;
    }

    function updateVirtualDateFieldsState() {
        const disabled = virtualCheckbox ? virtualCheckbox.checked : false;
        dateGroups.forEach(group => {
            const select = group.querySelector('.date-select-right');
            const checkbox = group.querySelector('.date-fulltext-checkbox');
            const input = group.querySelector('.date-input');
            if (!select || !input) return;
            if (disabled) {
                select.disabled = true;
                select.value = 'Original';
                if (checkbox) {
                   // checkbox.disabled = true;
                    //checkbox.checked = false;
                }
            } else {
                select.disabled = false;
                if (checkbox) checkbox.disabled = false;
            }
            updateCheckboxValue(group);
        });
    }

    dateGroups.forEach(group => {
        const select = group.querySelector('.date-select-right');
        if (select) {
            select.addEventListener('change', () => updateCheckboxValue(group));
        }
        updateCheckboxValue(group);
    });

    if (virtualCheckbox) {
        virtualCheckbox.addEventListener('change', updateVirtualDateFieldsState);
        updateVirtualDateFieldsState();
    }

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
    enforceRadioState();
});