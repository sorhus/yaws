
ps aux | grep MyPipe | awk '{print $2}' | xargs kill

