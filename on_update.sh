#! /bin/bash
DIRECTORY_TO_OBSERVE="/home/ubuntu/user_input"
function block_for_change {
  inotifywait -r \
    -e modify \
    $DIRECTORY_TO_OBSERVE
}
BUILD_SCRIPT=mapred.sh
function build {
  bash $BUILD_SCRIPT
}
while block_for_change; do
  build
done