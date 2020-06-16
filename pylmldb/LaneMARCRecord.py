#!/usr/bin/python3
# -*- coding: UTF-8 -*-

import unicodedata
import regex as re
from loguru import logger

from pymarc import Record, Field

from .VoyagerAPI import VoyagerAPI
from .xobis_constants import *


class LaneMARCRecord(Record):
    """
    Superclass of pymarc Record with Lane/XOBIS-specific functionality
    """

    def get_control_number(self):
        # Record control number (001 plus prefix letter; generated by RIM in 035 ^9)
        record_control_nos = self.get_subfields('035','9')
        if record_control_nos:
            return '(CStL)' + record_control_nos[0]
        elif '852' in self:
            # Holdings don't currently have them; build from 001 instead
            return '(CStL)H' + self['001'].data
        # @@@@@@@@ something wrong
        return None

    def get_primary_categories(self):
        return [val for field in self.get_fields('655') for val in field.get_subfields('a') if field.indicator1 == '1']

    def get_broad_category(self):
        # returns 655 47 ^a if record has exactly one, otherwise prints warning
        broad_categories = [field['a'] for field in self.get_fields('655') if field.indicator1 == '4']
        if not broad_categories:
            if '852' not in self:
                logger.warning(f"{self.get_control_number() or self['001'].data}: missing broad category (655 47)")
            return None
        elif len(broad_categories) > 1:
            logger.warning(f"{self.get_control_number() or self['001'].data}: more than one broad category (655 47)")
            return None
        return broad_categories.pop()

    def get_subsets(self):
        return [val for field in self.get_fields('655') for val in field.get_subfields('a') if field.indicator1 in '78']

    def get_all_categories(self):
        return [val for field in self.get_fields('655') for val in field.get_subfields('a') if field.indicator1 not in '78']

    def get_isbns(self):
        isbn_vals = self.get_subfields('020',('a','z')) + [val for field in self.get_fields('024') for val in field.get_subfields('a','z') if field.indicator1 == '3']
        # wipe all parentheticals and anything after colon/dollar sign/v; nums + X only
        return [re.sub(r'[^\dX]', '', re.sub(r'[\(:\$Vv].*', '', val).upper()) for val in isbn_vals]

    def get_issns(self):
        issn_vals = self.get_subfields('022',('a','l','m','y','z'))
        # wipe all parentheticals and anything after colon/dollar sign/v; nums + X only
        return [re.sub(r'[^\dX]', '', re.sub(r'[\(:\$Vv].*', '', val).upper()) for val in issn_vals]

    def is_referential(self):
        if '008' not in self:
            return None
        return self['008'].data[9] in 'bce' or 'Unestablished' in self.get_subsets()

    def is_monographic(self):
        return self.get_broad_category() in ("Book Sets", "Books", "Pamphlets", "Leaflets", "Documents", "Components")

    def is_suppressed(self):
        return 'Suppressed' in self.get_subsets()

    ID_FIELDS = ('149','100','110','111','130','150','151','155','180','182','852')

    def get_id_field(self):
        """
        Returns field containing the record's main entry/identity.
        """
        for tag in self.ID_FIELDS:
            if tag in self:
                return self[tag]
        return None

    def get_xobis_element_type(self, tag=None):
        """
        Returns a 3-letter code representing the XOBIS element type of a field
          with the given tag, in the context of this particular record
          (since this may vary depending on this record's type).
        Defaults to this record's identity field tag, and so in that case
          will return the element type of this record as a whole.
        """
        id_field = self.get_id_field()
        if id_field is None:
            return None
        id_tag = id_field.tag
        tag = tag or id_tag

        # if self.is_suppressed():
        #     return None

        # ~~~~~~ BIB ~~~~~~
        if tag in ('149','210','245','246','247','249'):
            return OBJECT if self.get_broad_category() == 'Objects' else WORK_INST
        # ~~~~~~ AUT ~~~~~~
        elif id_tag not in ('149','852') or tag[0] in '14':
            if tag.endswith('00'): return BEING
            elif tag.endswith('10'): return ORGANIZATION
            elif tag.endswith('11'): return EVENT
            elif tag.endswith('30'): return WORK_AUT
            elif tag.endswith('50'):
                broad_category = self.get_broad_category()
                if broad_category in ['Languages','Scripts']:
                    return LANGUAGE
                elif broad_category in ['Times']:
                    return TIME
                else:
                    return CONCEPT
            elif tag.endswith('51'): return PLACE
            elif tag.endswith('55'):
                broad_category = self.get_broad_category()
                if 'Relationship' in broad_category:
                    return 'rel'
                elif broad_category in ['Record Type','Subset','Relationship Type']:
                    return CONCEPT
                elif broad_category in ['Category']:
                    if 'Restricted Usage' in self.get_subfields('655','a'):
                        return CONCEPT
                    else:
                        # 155s derived from 150 Categories for validation purposes only
                        return None
            elif tag.endswith('80') or tag == '072': return CONCEPT
            elif tag.endswith('82'): return STRING
        # ~~~~~~ HDG ~~~~~~
        elif tag == '852': return HOLDINGS
        else:
            return None

    BIB, AUT, HDG = VoyagerAPI.BIB, VoyagerAPI.AUT, VoyagerAPI.HDG
    def get_record_type(self):
        return { WORK_INST: self.BIB,
                 OBJECT:    self.BIB,
                 HOLDINGS:  self.HDG,
                 None: None }.get(self.get_xobis_element_type(), self.AUT)

    def get_identity_information(self):
        """
        Returns control number, XOBIS element type, normalized identity string,
        and authorized-form main entry string for this record.
        """
        # if self.is_suppressed():
        #     return (), None, None, None
        ctrlno = self.get_control_number()
        # @@@@ TEMPORARY, HOLDINGS SHOULD HAVE AN IDENTITY EVENTUALLY @@@@
        if ctrlno is None or 'H' in ctrlno:
            return (), None, None, None
        # which field contains the identity?
        id_field = self.get_id_field()
        if id_field is None:
            if not self.is_suppressed():
                logger.error(f"{ctrlno}: no id field found")
            return (), None, None, None
        # get the identity
        element_type = self.get_xobis_element_type()
        identity_string = self.get_identity_from_field(id_field, element_type, normalized=True)
        authorized_form = self.get_identity_from_field(id_field, element_type, normalized=False) \
                              if identity_string else None
        return ctrlno, element_type, identity_string, authorized_form

    IDENTITY_SUBFIELD_MAP = { WORK_INST:    'adklnpqs',    # X49
                              OBJECT:       'adklnpqs',    # X49
                              # WORK_INST:    'adhklnpqs',   # X49 + h (medium)
                              # OBJECT:       'adhklnpqs',   # X49 + h (medium)
                              WORK_AUT:     'adfgklnpqs',  # X30
                              BEING:        'abcdq',       # X00
                              ORGANIZATION: 'abcdn',       # X10
                              EVENT:        'acden',       # X11
                              CONCEPT:      'ax',          # X50/X55/X80
                              TIME:         'ax',          # X50
                              LANGUAGE:     'a',           # X50
                              PLACE:        'az',          # X51
                              RELATIONSHIP: 'a',           # X55
                              STRING:       'yqg3' }       # X82

    variant_field_tags = ['072','130','150','210','245','246','247','249','400','410','411','430','450','451','455','480','482']
    def get_variant_fields(self):
        """
        Returns list of fields for variant entries of this record.
        """
        variant_fields = []
        for field in self.get_fields(*self.variant_field_tags):
            if field.tag == '150':
                if 'm' in field:
                    # add MeSH "as Topic" version as a variant
                    new_field = Field(field.tag, (field.indicator1, field.indicator2), field.subfields.copy())
                    new_field['a'] = new_field['m']
                    new_field.delete_all_subfields('m')
                    variant_fields.append(new_field)
            elif field.tag == '130':
                # only a variant for bibs
                if '245' in self:
                    variant_fields.append(field)
            else:
                variant_fields.append(field)
        return variant_fields

    def get_variant_fields_and_types(self):
        """
        Returns list of fields and their element types
        for variant entries of this record.
        """
        variant_fields_and_types = []
        for variant_field in self.get_variant_fields():
            element_type = self.get_xobis_element_type(variant_field.tag)
            variant_fields_and_types.append((variant_field, element_type))
        return variant_fields_and_types

    def get_variant_types_and_ids(self, normalized=True):
        """
        Returns list of element types and identity strings
        for variant entries of this record.
        """
        variant_types_and_ids = set()
        for variant_field, element_type in self.get_variant_fields_and_types():
            variant_id_string = self.get_identity_from_field( variant_field, \
                                                              element_type, \
                                                              normalized )
            variant_types_and_ids.add((element_type, variant_id_string))
        return list(variant_types_and_ids)


    DIGITAL, PHYSICAL, COMPONENT = 'digi', 'phys', 'comp'
    def get_holdings_type(self):
        """
        Does this record represent digital, physical, or component holdings?
        """
        if self.get_xobis_element_type() != HOLDINGS:
            return None
        # determine from 852 $b location code
        location_code = self['852']['b']
        if location_code is None:
            return None
        elif location_code in ('COMP','RLOC'):
            return self.COMPONENT
        elif location_code in ('DTBS','ECOLL','ECOMP','EDATA','EDOC','EFEE','EPER','EPER1','EPER2','EPER3','EPER4','EPER5','EPER6','IMAGE','ISIIF','MOBI'):
            return self.DIGITAL
        return self.PHYSICAL



    RELATOR_SUBF_i_TAGS = ('246','411')

    NORMALIZED_SEP = ','
    UNNORMALIZED_SEP = '\t'

    @classmethod
    def get_identity_from_field(cls, field, element_type, normalized=True):
        """
        Generates identity string from given pymarc field and XOBIS type.
        """
        # which subfields should be included as part of the identity?
        if element_type not in cls.IDENTITY_SUBFIELD_MAP:
            return None
        elif '760' <= field.tag <= '789':
            # exception for bib linking entry fields
            if 't' in field:
                subfield_codes = 'tb'
            elif 'p' in field:
                subfield_codes = 'pb'
            else:
                subfield_codes = 'ab'
            if field.tag in ('787','789'):
                subfield_codes += 'd'
        else:
            subfield_codes = cls.IDENTITY_SUBFIELD_MAP[element_type]
        # pull those subfields to generate it
        if normalized:
            # normalization also standardizes subfield order and inclusion
            identity = []
            for code in subfield_codes:
                if code in field:
                    for value in field.get_subfields(code):
                        identity.append(code)
                        identity.append(cls.normalize(value))
                else:
                    identity.append(code)
                    identity.append('')
            sep = cls.NORMALIZED_SEP
        else:
            # if no normalization, only include what's in the field, in the original order
            # this isn't really an "identity," it's really being used to record an authorized form
            identity = [code_or_val for code_and_val in field.get_subfields(*subfield_codes, with_codes=True) for code_or_val in code_and_val]
            sep = cls.UNNORMALIZED_SEP
        if not ''.join(identity[1::2]):
            return None
        return sep.join(identity)

    @staticmethod
    def normalize(text):
        """
        Normalization algorithm for indexing:
        1. Unicode NFKD normalize.
        2. Convert anything of Unicode General Category
           Punctuation (P*), Separator (Z*), or Other (C*)
           to single spaces.
           (Requires the `regex` module, `re` cannot use \p)
        3. Strip.
        4. Convert to lowercase.
        """
        return re.sub(r"[\p{P}\p{Z}\p{C}]+", ' ', unicodedata.normalize('NFKD', text)).strip().lower()
