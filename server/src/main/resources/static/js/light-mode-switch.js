/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

const lightSwitch = document.getElementById("lightSwitch");
window.addEventListener("load", function () {
    if (lightSwitch) {
        initTheme();
        lightSwitch.addEventListener("change", function () {
            resetTheme();
        });
    }
});

/**
 * Summary: function that adds or removes the attribute 'data-theme' depending if
 * the switch is 'on' or 'off'.
 *
 * Description: initTheme is a function that uses localStorage from JavaScript DOM,
 * to store the value of the HTML switch. If the switch was already switched to
 * 'on' it will set an HTML attribute to the body named: 'data-theme' to a 'light'
 * value. If it is the first time opening the page, or if the switch was off the
 * 'data-theme' attribute will not be set.
 * @return {void}
 */
function initTheme() {
    const lightThemeSelected =
        localStorage.getItem("lightSwitch") !== null &&
        localStorage.getItem("lightSwitch") === "light";
    lightSwitch.checked = lightThemeSelected;
    lightThemeSelected
        ? document.body.setAttribute("data-theme", "light")
        : document.body.removeAttribute("data-theme");
}

/**
 * Summary: resetTheme checks if the switch is 'on' or 'off' and if it is toggled
 * on it will set the HTML attribute 'data-theme' to light so the light-theme CSS is
 * applied.
 * @return {void}
 */
function resetTheme() {
    if (lightSwitch.checked) {
        document.body.setAttribute("data-theme", "light");
        localStorage.setItem("lightSwitch", "light");
    } else {
        document.body.removeAttribute("data-theme");
        localStorage.removeItem("lightSwitch");
    }
}
