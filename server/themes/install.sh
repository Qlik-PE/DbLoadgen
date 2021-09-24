#!/bin/bash
#

cd "$(dirname "$0")"

current_dir=${PWD##*/}

if [ $# -ne 1 ]
then
   echo "ERROR: no theme directory specified"
   echo ""
   echo "Usage: install.sh <theme-subdirectory>"
   echo ""
   exit 1
fi


if [[ ! -d "$1" ]]
then
   echo "ERROR: Invalid theme directory specified: $1"
   exit 1
fi
theme="$1"
theme_source_dir="$current_dir/$theme"

bootswatch="./$theme/_bootswatch.scss" 
variables="./$theme/_variables.scss"

if [[ ! -f "$bootswatch" ]]
then
   echo "ERROR: file $bootswatch does not exist"
   exit 1
fi

if [[ ! -f "$variables" ]]
then
   echo "ERROR: file $variables does not exist"
   exit 1
fi

if ! command -v npm > /dev/null
then
  echo "ERROR: npm not installed"
  exit 1
fi

if ! command -v grunt > /dev/null
then
  echo "ERROR: grunt not installed; run 'npm install -g grunt-cli'"
  exit 1
fi

set -e
cd ..
mkdir -p target
cd target
if [ -d bootswatch ]; then
  echo "Updating bootswatch"
  cd bootswatch
  git pull
else
  echo "Cloning bootswatch"
  git clone https://github.com/thomaspark/bootswatch.git
  cd bootswatch
fi

mkdir -p dist/loadgen
cp ../../"$theme_source_dir"/*.scss dist/loadgen

echo "Building theme from $theme_source_dir"
npm install
grunt swatch:loadgen
theme_target_dir=src/main/resources/static/css
cp dist/loadgen/bootstrap.css ../../$theme_target_dir

echo "Theme installed into $theme_target_dir"

