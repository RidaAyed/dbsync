Build Package
---------------------------------------------

1. Ins Verzeichnis build wechseln

 $ cd build

2. Aktuelle Version auschecken

 $ dh-make-golang bitbucket.org/modima/dbsync

3. Changelog aktualisieren

 $ nano dbsync/debian/changelog




1. Changelog erweitern / Was wurde geändert

 $ nano debian/changelog


2. Änderungen einchecken

 $ git add * && git commit -a -m "..."
 $ git push origin master


3. 
