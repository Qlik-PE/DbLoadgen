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

window.onload = hideHome;

function hideHome() {
   const currentPathname = window.location.pathname;
   const x  = document.getElementById("homeLink");
   const y  = document.getElementById("hideHome");

   /*
    * Only display if we are not on the home page.
    * Need to account for context path.
    */

   if (!currentPathname.endsWith('/')  && !currentPathname.endsWith('/login')) {
      x.style.display = "block";
      y.style.display = "none";
   } else {
      x.style.display = "none";
      y.style.display = "block";
   }
}

