<#--
 Licensed under the Apache License, Version 2.0 (the "License");
 you may not use this file except in compliance with the License.
 You may obtain a copy of the License at

 http://www.apache.org/licenses/LICENSE-2.0

 Unless required by applicable law or agreed to in writing, software
 distributed under the License is distributed on an "AS IS" BASIS,
 WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 See the License for the specific language governing permissions and
 limitations under the License.
-->
<#import "/spring.ftl" as spring />

<div class="pb-2 mt-5 mb-4 border-bottom border-secondary">
    <div class="container">
        <div class="container-fluid pl-0">
            <div id="header-title-line" class="row">
                <div id="logo" class="col-md-1">
                    <img alt="logo" height="100%" src="<@spring.url '/images/qlik-logo-rgb.png'/>"/>
                </div>
                <div class="col-md-10 text-center">
                    <h1 class="app-name brand mb-0">
                        DbLoadgen
                    </h1>
                </div>
                <div class="form-check form-switch col-md-1">
                    <input type="checkbox" class="form-check-input text-right" id="lightSwitch"/>
                    <label class="form-check-label" for="lightSwitch"><i class="fa fa-lightbulb-o"></i></label>
                </div>
                <script src="<@spring.url '/js/light-mode-switch.min.js'/>"></script>
            </div>
            <div id="homeLink">
                <h4>
                    <a href="<@spring.url '/'/>">Home</a>
                </h4>
            </div>
            <div id="hideHome">
                <h4>&nbsp;</h4>
            </div>
        </div>
    </div>
</div>
