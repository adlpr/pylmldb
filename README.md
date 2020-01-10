# pylmldb

Package for pulling LMLDB MARC data from Voyager and reading/storing it locally.

## Classes

* `pylmldb.VoyagerAPI` : Interface for pulling current MARC data from the Lane Voyager HTTPS API
* `pylmldb.LaneMARCRecord` : Superclass of `pymarc.Record` with Lane/XOBIS-specific functionality
* `pylmldb.LMLDB` : Interface for creating/accessing a (local) postgres mirror of the Lane MARC catalog
