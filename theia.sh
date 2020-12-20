#!/bin/sh

docker run -dit -p 3000:3000 -v $(pwd):/home/project:rw -u 1000:1000 theiaide/theia-python
