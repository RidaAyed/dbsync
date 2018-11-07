Build Package
---------------------------------------------
0. Tag für aktuelle Version erstellen

 $ git tag 1.0.1 && git push origin 1.0.1

1. Eventuell Build-Verzeichnis anlegen

 $ mkdir build && cd build

2. Aktuelle Version aus Repository laden

 $ dh-make-golang bitbucket.org/modima/dbsync 

3. Debian Verzeichnis kopieren (da es in Schritt 2 neu erzeugt wurde)

 $ cp -r ../debian/ dbsync/

4. Changelog aktualisieren (Version muss orig.tar.gz entsprechen)

 $ nano dbsync/debian/changelog

5. Changelog zurückkopieren

 $ cp -r dbsync/debian/ ../

6. Debian SOURCE Paket bauen (Option -S unbedingt verwenden, sonst wird es später von launchpad nicht akzeptiert)

 $ cd dbsync && debuild -S

7. Paket ins PPA laden

 $ dput ppa:cloud-it/ppa ../*.changes

8, Upload und Build Status prüfen auf https://launchpad.net/~cloud-it/+archive/ubuntu/ppa
