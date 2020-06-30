# pylmldb

Tools for fetching and manipulating LMLDB MARC data from Voyager.

## Classes

* `pylmldb.VoyagerAPI` : Interface for pulling current MARC data from the Lane Voyager HTTPS API
* `pylmldb.LaneMARCRecord` : Superclass of `pymarc.Record` with Lane/XOBIS-specific functionality
* `pylmldb.LMLDB` : Interface for creating/accessing a (local) mirror of the Lane MARC catalog
* `pylmldb.Surveyor` : Abstracted report generator