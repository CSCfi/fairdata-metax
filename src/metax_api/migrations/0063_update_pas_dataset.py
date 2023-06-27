from django.db import migrations

import logging

logger = logging.getLogger(__name__)


def update_pas_dataset(apps, schema_editor):
	logger.info("Updating metadata of a PAS dataset")

	CatalogRecord = apps.get_model("metax_api", "CatalogRecord")
	cr_id = "b4650072-73e4-4dfc-a2fa-c19bd4c97c10"

	creator_new = [
		{
			"name": {
				"fi": "HKTL-arkisto",
				"en": "SHCAS Archives"
			},
			"@type": "Organization",
			"is_part_of": {
				"name": {
					"en": "University of Turku",
					"fi": "Turun yliopisto",
					"sv": "Åbo universitet",
					"und": "Turun yliopisto"
				},
				"@type": "Organization",
				"identifier": "http://uri.suomi.fi/codelist/fairdata/organization/code/10089"
			},
			"homepage": {
				"title": {
					"en": "Creator website",
					"fi": "Tekijän kotisivu"
				},
				"identifier": "https://www.utu.fi/fi/yliopisto/humanistinen-tiedekunta/hkt-arkisto"
			}
		}
	]

	field_of_science_new = [
		{
			"in_scheme": "http://www.yso.fi/onto/okm-tieteenala/conceptscheme",
			"definition": {
				"en": "A statement or formal explanation of the meaning of a concept."
			},
			"identifier": "http://www.yso.fi/onto/okm-tieteenala/ta616",
			"pref_label": {
				"en": "Other humanities",
				"fi": "Muut humanistiset tieteet",
				"sv": "Övriga humanistiska vetenskaper",
				"und": "Muut humanistiset tieteet"
			}
		}
	]

	temporal_new = [
		{
			"end_date": "2002-12-31T00:00:01Z",
			"start_date": "1965-01-01T00:00:01Z"
		}
	]

	spatial_new = [
		{
			"place_uri": {
				"in_scheme": "http://www.yso.fi/onto/yso/places",
				"identifier": "http://www.yso.fi/onto/yso/p94459",
				"pref_label": {
					"en": "Utsjoki",
					"fi": "Utsjoki",
					"sv": "Utsjoki",
					"und": "Utsjoki"
				}
			}
		}
	]

	theme_new = [
		{
			"in_scheme": "http://www.yso.fi/onto/koko/",
			"identifier": "http://www.yso.fi/onto/koko/p8679",
			"pref_label": {
				"en": "arctic cultures",
				"fi": "arktiset kulttuurit",
				"se": "árktalaš kultuvrrat",
				"sv": "arktiska kulturer",
				"und": "arktiset kulttuurit"
			}
		},
		{
			"in_scheme": "http://www.yso.fi/onto/koko/",
			"identifier": "http://www.yso.fi/onto/koko/p31100",
			"pref_label": {
				"de": "Feldforschung",
				"en": "field work",
				"et": "välitööd",
				"fi": "kenttätyö",
				"se": "gieddebargu",
				"sv": "fältarbete",
				"und": "kenttätyö"
			}
		},
		{
			"in_scheme": "http://www.yso.fi/onto/koko/",
			"identifier": "http://www.yso.fi/onto/koko/p35965",
			"pref_label": {
				"en": "folk medicine",
				"fi": "kansanlääkintä",
				"sv": "folkmedicin",
				"und": "kansanlääkintä"
			}
		},
		{
			"in_scheme": "http://www.yso.fi/onto/koko/",
			"identifier": "http://www.yso.fi/onto/koko/p35386",
			"pref_label": {
				"de": "Folklore",
				"en": "folklore",
				"et": "folkloor",
				"fi": "kansanperinne",
				"se": "álbmotárbevierru",
				"sv": "folktradition",
				"und": "kansanperinne"
			}
		},
		{
			"in_scheme": "http://www.yso.fi/onto/koko/",
			"identifier": "http://www.yso.fi/onto/koko/p36929",
			"pref_label": {
				"en": "means of livelihood",
				"fi": "elinkeinot",
				"se": "ealáhusat",
				"sv": "näringar",
				"und": "elinkeinot"
			}
		},
		{
			"in_scheme": "http://www.yso.fi/onto/koko/",
			"identifier": "http://www.yso.fi/onto/koko/p52355",
			"pref_label": {
				"en": "oral tradition",
				"fi": "suullinen perinne",
				"se": "njálmmálaš árbevierru",
				"sv": "muntlig tradition",
				"und": "suullinen perinne"
			}
		},
		{
			"in_scheme": "http://www.yso.fi/onto/koko/",
			"identifier": "http://www.yso.fi/onto/koko/p34399",
			"pref_label": {
				"en": "religion and religions",
				"fi": "uskonto ja uskonnot",
				"se": "oskkoldat ja oskkoldagat",
				"sv": "religion och religioner",
				"und": "uskonto ja uskonnot"
			}
		},
		{
			"in_scheme": "http://www.yso.fi/onto/koko/",
			"identifier": "http://www.yso.fi/onto/koko/p32570",
			"pref_label": {
				"en": "Samis",
				"fi": "saamelaiset",
				"se": "sápmelaččat",
				"sv": "samer",
				"und": "saamelaiset"
			}
		},
		{
			"in_scheme": "http://www.yso.fi/onto/koko/",
			"identifier": "http://www.yso.fi/onto/koko/p34224",
			"pref_label": {
				"en": "supernatural creatures",
				"fi": "yliluonnolliset olennot",
				"se": "badjellunddolaš sivdnádusat",
				"sv": "övernaturliga väsen",
				"und": "yliluonnolliset olennot"
			}
		},
		{
			"in_scheme": "http://www.yso.fi/onto/koko/",
			"identifier": "http://www.yso.fi/onto/koko/p32599",
			"pref_label": {
				"en": "tradition",
				"fi": "perinne",
				"se": "árbevierru",
				"sv": "tradition",
				"und": "perinne"
			}
		},
		{
			"in_scheme": "http://www.yso.fi/onto/koko/",
			"identifier": "http://www.yso.fi/onto/koko/p33424",
			"pref_label": {
				"en": "village research",
				"fi": "kylätutkimus",
				"se": "gilidutkamuš",
				"sv": "byundersökning",
				"und": "kylätutkimus"
			}
		}
	]

	contributor_new = [
		{
			"name": "Lauri Honko",
			"@type": "Person",
			"member_of": {
				"name": {
					"en": "University of Turku",
					"fi": "Turun yliopisto",
					"sv": "Åbo universitet",
					"und": "Turun yliopisto"
				},
				"@type": "Organization",
				"identifier": "http://uri.suomi.fi/codelist/fairdata/organization/code/10089"
			},
			"contributor_role": [
				{
					"in_scheme": "http://uri.suomi.fi/codelist/fairdata/contributor_role",
					"identifier": "http://uri.suomi.fi/codelist/fairdata/contributor_role/code/resources",
					"pref_label": {
						"en": "Resources",
						"fi": "Aineiston hankinta",
						"sv": "Material",
						"und": "Aineiston hankinta"
					}
				}
			]
		},
		{
			"name": "Juha Pentikäinen",
			"@type": "Person",
			"member_of": {
				"name": {
					"en": "University of Turku",
					"fi": "Turun yliopisto",
					"sv": "Åbo universitet",
					"und": "Turun yliopisto"
				},
				"@type": "Organization",
				"identifier": "http://uri.suomi.fi/codelist/fairdata/organization/code/10089"
			},
			"contributor_role": [
				{
					"in_scheme": "http://uri.suomi.fi/codelist/fairdata/contributor_role",
					"identifier": "http://uri.suomi.fi/codelist/fairdata/contributor_role/code/resources",
					"pref_label": {
						"en": "Resources",
						"fi": "Aineiston hankinta",
						"sv": "Material",
						"und": "Aineiston hankinta"
					}
				}
			]
		},
		{
			"name": "Helvi Nuorgam-Poutasuo",
			"@type": "Person",
			"member_of": {
				"name": {
					"en": "University of Turku",
					"fi": "Turun yliopisto",
					"sv": "Åbo universitet",
					"und": "Turun yliopisto"
				},
				"@type": "Organization",
				"identifier": "http://uri.suomi.fi/codelist/fairdata/organization/code/10089"
			},
			"contributor_role": [
				{
					"in_scheme": "http://uri.suomi.fi/codelist/fairdata/contributor_role",
					"identifier": "http://uri.suomi.fi/codelist/fairdata/contributor_role/code/resources",
					"pref_label": {
						"en": "Resources",
						"fi": "Aineiston hankinta",
						"sv": "Material",
						"und": "Aineiston hankinta"
					}
				}
			]
		},
		{
			"name": "Olavi Korhonen",
			"@type": "Person",
			"member_of": {
				"name": {
					"en": "University of Turku",
					"fi": "Turun yliopisto",
					"sv": "Åbo universitet",
					"und": "Turun yliopisto"
				},
				"@type": "Organization",
				"identifier": "http://uri.suomi.fi/codelist/fairdata/organization/code/10089"
			},
			"contributor_role": [
				{
					"in_scheme": "http://uri.suomi.fi/codelist/fairdata/contributor_role",
					"identifier": "http://uri.suomi.fi/codelist/fairdata/contributor_role/code/resources",
					"pref_label": {
						"en": "Resources",
						"fi": "Aineiston hankinta",
						"sv": "Material",
						"und": "Aineiston hankinta"
					}
				}
			]
		},
		{
			"name": "Matti Morottaja",
			"@type": "Person",
			"member_of": {
				"name": {
					"en": "University of Turku",
					"fi": "Turun yliopisto",
					"sv": "Åbo universitet",
					"und": "Turun yliopisto"
				},
				"@type": "Organization",
				"identifier": "http://uri.suomi.fi/codelist/fairdata/organization/code/10089"
			},
			"contributor_role": [
				{
					"in_scheme": "http://uri.suomi.fi/codelist/fairdata/contributor_role",
					"identifier": "http://uri.suomi.fi/codelist/fairdata/contributor_role/code/resources",
					"pref_label": {
						"en": "Resources",
						"fi": "Aineiston hankinta",
						"sv": "Material",
						"und": "Aineiston hankinta"
					}
				}
			]
		},
		{
			"name": "Erkki Itkonen",
			"@type": "Person",
			"member_of": {
				"name": {
					"en": "University of Turku",
					"fi": "Turun yliopisto",
					"sv": "Åbo universitet",
					"und": "Turun yliopisto"
				},
				"@type": "Organization",
				"identifier": "http://uri.suomi.fi/codelist/fairdata/organization/code/10089"
			},
			"contributor_role": [
				{
					"in_scheme": "http://uri.suomi.fi/codelist/fairdata/contributor_role",
					"identifier": "http://uri.suomi.fi/codelist/fairdata/contributor_role/code/resources",
					"pref_label": {
						"en": "Resources",
						"fi": "Aineiston hankinta",
						"sv": "Material",
						"und": "Aineiston hankinta"
					}
				}
			]
		},
		{
			"name": "Anne Suomi",
			"@type": "Person",
			"member_of": {
				"name": {
					"en": "University of Turku",
					"fi": "Turun yliopisto",
					"sv": "Åbo universitet",
					"und": "Turun yliopisto"
				},
				"@type": "Organization",
				"identifier": "http://uri.suomi.fi/codelist/fairdata/organization/code/10089"
			},
			"contributor_role": [
				{
					"in_scheme": "http://uri.suomi.fi/codelist/fairdata/contributor_role",
					"identifier": "http://uri.suomi.fi/codelist/fairdata/contributor_role/code/resources",
					"pref_label": {
						"en": "Resources",
						"fi": "Aineiston hankinta",
						"sv": "Material",
						"und": "Aineiston hankinta"
					}
				}
			]
		},
		{
			"name": "Lassi Saressalo",
			"@type": "Person",
			"member_of": {
				"name": {
					"en": "University of Turku",
					"fi": "Turun yliopisto",
					"sv": "Åbo universitet",
					"und": "Turun yliopisto"
				},
				"@type": "Organization",
				"identifier": "http://uri.suomi.fi/codelist/fairdata/organization/code/10089"
			},
			"contributor_role": [
				{
					"in_scheme": "http://uri.suomi.fi/codelist/fairdata/contributor_role",
					"identifier": "http://uri.suomi.fi/codelist/fairdata/contributor_role/code/resources",
					"pref_label": {
						"en": "Resources",
						"fi": "Aineiston hankinta",
						"sv": "Material",
						"und": "Aineiston hankinta"
					}
				}
			]
		},
		{
			"name": "Pekka Sammallahti",
			"@type": "Person",
			"member_of": {
				"name": {
					"en": "University of Turku",
					"fi": "Turun yliopisto",
					"sv": "Åbo universitet",
					"und": "Turun yliopisto"
				},
				"@type": "Organization",
				"identifier": "http://uri.suomi.fi/codelist/fairdata/organization/code/10089"
			},
			"contributor_role": [
				{
					"in_scheme": "http://uri.suomi.fi/codelist/fairdata/contributor_role",
					"identifier": "http://uri.suomi.fi/codelist/fairdata/contributor_role/code/resources",
					"pref_label": {
						"en": "Resources",
						"fi": "Aineiston hankinta",
						"sv": "Material",
						"und": "Aineiston hankinta"
					}
				}
			]
		},
		{
			"name": "Marjut Huuskonen",
			"@type": "Person",
			"member_of": {
				"name": {
					"en": "University of Turku",
					"fi": "Turun yliopisto",
					"sv": "Åbo universitet",
					"und": "Turun yliopisto"
				},
				"@type": "Organization",
				"identifier": "http://uri.suomi.fi/codelist/fairdata/organization/code/10089"
			},
			"contributor_role": [
				{
					"in_scheme": "http://uri.suomi.fi/codelist/fairdata/contributor_role",
					"identifier": "http://uri.suomi.fi/codelist/fairdata/contributor_role/code/resources",
					"pref_label": {
						"en": "Resources",
						"fi": "Aineiston hankinta",
						"sv": "Material",
						"und": "Aineiston hankinta"
					}
				}
			]
		},
		{
			"name": "Pasi Enges",
			"@type": "Person",
			"member_of": {
				"name": {
					"en": "University of Turku",
					"fi": "Turun yliopisto",
					"sv": "Åbo universitet",
					"und": "Turun yliopisto"
				},
				"@type": "Organization",
				"identifier": "http://uri.suomi.fi/codelist/fairdata/organization/code/10089"
			},
			"identifier": "https://orcid.org/0000-0002-4231-2163",
			"contributor_role": [
				{
					"in_scheme": "http://uri.suomi.fi/codelist/fairdata/contributor_role",
					"identifier": "http://uri.suomi.fi/codelist/fairdata/contributor_role/code/resources",
					"pref_label": {
						"en": "Resources",
						"fi": "Aineiston hankinta",
						"sv": "Material",
						"und": "Aineiston hankinta"
					}
				}
			]
		},
		{
			"name": "Jouni Vest",
			"@type": "Person",
			"member_of": {
				"name": {
					"en": "University of Turku",
					"fi": "Turun yliopisto",
					"sv": "Åbo universitet",
					"und": "Turun yliopisto"
				},
				"@type": "Organization",
				"identifier": "http://uri.suomi.fi/codelist/fairdata/organization/code/10089"
			},
			"contributor_role": [
				{
					"in_scheme": "http://uri.suomi.fi/codelist/fairdata/contributor_role",
					"identifier": "http://uri.suomi.fi/codelist/fairdata/contributor_role/code/resources",
					"pref_label": {
						"en": "Resources",
						"fi": "Aineiston hankinta",
						"sv": "Material",
						"und": "Aineiston hankinta"
					}
				}
			]
		}
	]

	try:
		cr = CatalogRecord.objects.get(identifier=cr_id)
		cr.research_dataset["creator"] = creator_new
		cr.research_dataset["field_of_science"] = field_of_science_new
		cr.research_dataset["temporal"] = temporal_new
		cr.research_dataset["spatial"] = spatial_new
		cr.research_dataset["theme"] = theme_new
		cr.research_dataset["contributor"] = contributor_new
		cr.save()
		logger.info(f"cr: {cr} updated")

	except CatalogRecord.DoesNotExist:
		logger.info(f"Catalog record: {cr_id} not found. Do nothing.")


def revert(apps, schema_editor):
	logger.info("Reverting metadata of a PAS dataset")

	CatalogRecord = apps.get_model("metax_api", "CatalogRecord")
	cr_id = "9837f19a-4b48-4774-a9b8-fad5058de6a2"

	creator_old = [
		{
			"name": "HKT-arkisto/Heli Syrjälä",
			"@type": "Person",
			"member_of": {
				"name": {
					"en": "University of Turku",
					"fi": "Turun yliopisto",
					"sv": "Åbo universitet",
					"und": "Turun yliopisto"
				},
				"@type": "Organization",
				"identifier": "http://uri.suomi.fi/codelist/fairdata/organization/code/10089"
			}
		}
	]

	try:
		cr = CatalogRecord.objects.get(identifier=cr_id)
		cr.research_dataset["creator"] = creator_old
		cr.research_dataset.pop("field_of_science", None)
		cr.research_dataset.pop("temporal", None)
		cr.research_dataset.pop("spatial", None)
		cr.research_dataset.pop("theme", None)
		cr.research_dataset.pop("contributor", None)
		cr.save()
		logger.info(f"cr: {cr} reverted")
	except CatalogRecord.DoesNotExist:
		logger.info(f"Catalog record: {cr_id} not found. Do nothing.")


class Migration(migrations.Migration):
	dependencies = [
		("metax_api", "0062_auto_20230510_0919"),
	]

	operations = [
		migrations.RunPython(update_pas_dataset, revert),
	]
