Build Package
---------------------------------------------
0. Versionstag für aktuelle Version erstellen

 - https://bitbucket.org/modima/dbsync/commits/
 - Create tag (rechts)

1. Build Verzeichnis anlegen

 $ mkdir build && cd build

2. Aktuelle Version aus Repository laden

 $ dh-make-golang bitbucket.org/modima/dbsync 

3. Debian Verzeichnis kopieren (da es in Schritt 2 neu erzeugt wurde)

 $ cp -r ../debian/ dbsync/

4. Changelog aktualisieren (Version muss orig.tar.gz entsprechen)

 $ nano dbsync/debian/changelog

5. Debian SOURCE Paket bauen (Option -S unbedingt verwenden, sonst wird es später von launchpad nicht akzeptiert)

 $ cd dbsync && debuild -S

6. Paket ins PPA laden

 $ dput ppa:dialfire/ppa ../*.changes
