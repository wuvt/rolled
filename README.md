# rolled

Return Of Librarian Len's Electronic Database (ROLLED) is the latest advent in disk-locating technology.

## usage

There are four fundamental parts of this software:
* the ingestion scripts
* a postgres database
* a typesense node
* the frontend

The ingestion and frontend are containerized, and configured entirely via environment variables: refer to the table below to set everything up to your liking.

| environment variable | purpose |
| -------------------- | ------- |
| ING_POSTGRES_HOST    | pg endpoint |
| ING_POSTGRES_PORT    | ingress pg port |
| ING_POSTGRES_USERNAME    | ingress pg username |
| ING_POSTGRES_PASSWORD    | ingress pg password. secret. |
| ING_POSTGRES_DATABASE    | ingress pg database name |
| ING_TYPESENSE_HOST    | ingress typesense node. this software does not support a multi-node search cluster at the moment. |
| ING_TYPESENSE_PORT    | ingress typesense port. see above re: clusters. |
| ING_TYPESENSE_BOOTSTRAP_API_KEY    | initial/admin api key. secret. |
| ING_TYPESENSE_SEARCH_API_KEY    | generated search/read only API key. this will be exposed on the frontend. |
| FE_TYPESENSE_HOST | typesense backend endpoint relative to client |
| FE_TYPESENSE_PORT | typesense backend port relative to client |
| FE_TYPESENSE_SEARCHKEY | generated search/read only API key. this will be exposed on the frontend. |

The endpoints prefixed `ING_` are relative to the *ingestion container*. The ones prefixed `FE_` are relative to the *client*. Keep this in mind as you dockerize or k8s-ize or whatever: the client does not have cluster networking (probably. you might be doing something cursed. not my business if so.).

Furthermore, the ingestion scripts expect `/data` to contain a `/data/sheets` with the source spreadsheets. Cached MusicBrainz API content will be read and written to `/data/manifest.json`, so if you're bringing your own mount that as well.

### prod

In production, you will probably be inclined to use a non-local postgres or typesense database.

### dev

For development purposes, it has all been containerized in a single docker compose file: simply construct a dev.env file in the root of this repository following this format:
```
POSTGRES_USER=root
POSTGRES_PASSWORD=<some entropic password. or 'aids'. whichever you prefer.>
TYPESENSE_API_KEY=<some string, which will become the typesense bootstrap/admin api key>
ING_POSTGRES_PASSWORD=${POSTGRES_PASSWORD}
ING_TYPESENSE_BOOTSTRAP_API_KEY=${TYPESENSE_API_KEY}
ING_TYPESENSE_SEARCH_API_KEY=<some string, which will become the search-only key exposed by 
the frontend>
FE_TYPESENSE_SEARCHKEY=${ING_TYPESENSE_SEARCH_API_KEY}
```

place your spreadsheets in `./ingestion_data/sheets/`, run `docker compose up`, then open localhost:1337 in the web browser of your choice.


## contributing

WUVT IT is always looking for helping hands. Hit us up.

This software is licensed & available in its entirety under the GNU GPL.

```
Copyright 2025 WUVT-FM et al.

This program is free software: you can redistribute it and/or modify it under the terms of the GNU General Public License as published by the Free Software Foundation, either version 3 of the License, or (at your option) any later version.

This program is distributed in the hope that it will be useful, but WITHOUT ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the GNU General Public License for more details.

You should have received a copy of the GNU General Public License along with this program. If not, see <https://www.gnu.org/licenses/>. 
```

## roadmap
* spread the scope to include liners/promos/IDs/etc.
* allow digital playback of media for which there is a digital copy
    * alexandria http over the local network (which already exists)
    * symlink directory /tank/library/rolled/\<mbid\>/\<track#\>.flac
        * should be piss easy to add a HEAD req check during ingestion to check for album existence
        * then likewise easy to generate a link to the nth track symlink
* direct injection of "playback" into the airchain? everyone jacks into the board anyway...        
* pages for artists, maybe. /artist/mbid, pulls in a picture/desc/albums (with art)
* pages for albums, then, i guess. you get the idea.
