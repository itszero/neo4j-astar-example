# neo4j-astar-example

## Purpose

This projects implements A* path-finding algorithm, overlay network for
speeding up and a bunch of tool to import OpenStreetMap data as a special graph
representation. It's not meant to be a directly usable plugin. It's more like a
code example of how things could be done via Neo4j's plugin architecture and
web services. It was done for a class project, so the code did not focus on the
security issues (as you can see, the import function actually take a path on
the server and open it.)

I stumbled on lots of stones and quirks when I writing these codes. I hope by
opening the project it could help people to get onboard easier.

## License

MIT License. See `LICENSE`.

## Author

Zero Cho (itszero at gmail dot com)
