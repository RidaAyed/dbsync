Build Package
---------------------------------------------
1. Build Verzeichnis löschen

    $ rm -rf build

2. Tag für aktuelle Version erstellen

    $ git tag 1.0.1 && git push origin 1.0.1

3. Build-Verzeichnis anlegen

    $ mkdir build && cd build

4. Aktuelle Version aus Repository laden

    $ dh-make-golang bitbucket.org/modima/dbsync 

5. Debian Verzeichnis kopieren (da es in Schritt 2 neu erzeugt wurde)

    $ cp -r ../debian/ dbsync/

6. Changelog aktualisieren (Version muss orig.tar.gz entsprechen)

    $ nano dbsync/debian/changelog

7. Changelog zurückkopieren

    $ cp -r dbsync/debian/ ../

8. Debian SOURCE Paket bauen (Option -S unbedingt verwenden, sonst wird es später von launchpad nicht akzeptiert)

    $ cd dbsync && debuild -S

9. Paket ins PPA laden

    $ dput ppa:cloud-it/ppa ../*.changes

10. Upload und Build Status prüfen auf https://launchpad.net/~cloud-it/+archive/ubuntu/ppa
