<resource xsi:schemaLocation="http://datacite.org/schema/kernel-4 http://schema.datacite.org/meta/kernel-4.1/metadata.xsd" xmlns="http://datacite.org/schema/kernel-4" xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance" xmlns:mrd="http://uri.suomi.fi/datamodel/ns/mrd#">
    <identifier>{ mrd:researchdataset/mrd:preferred_identifier/text() }</identifier>
    <alternateIdentifier alternateIdentifierType="URN">
        { mrd:researchdataset/mrd:metadata_version_identifier/text() }
    </alternateIdentifier>
    <alternateIdentifier alternateIdentifierType="preferred_identifier">
        { mrd:researchdataset/mrd:preferred_identifier/text() }
    </alternateIdentifier>
    <dates>
        <date dateType="Updated">{ mrd:researchdataset/mrd:modified/text() }</date>
        <publicationYear>{ substring(mrd:researchdataset/mrd:modified/text(), 0 , 5) }</publicationYear>
        <date dateType="Issued">{ mrd:researchdataset/mrd:issued/text() }</date>
    </dates>
    <version>{ mrd:researchdataset/mrd:version_info/text() }</version>
    <creators>
    {
        for $creator in mrd:researchdataset/mrd:creator/mrd:item return
        <creator>
            <creatorName>{ $creator/mrd:name/text() }</creatorName>
            <nameIdentifier>{$creator/mrd:identifier/text()}</nameIdentifier>
            {
              for $label in $creator/mrd:member_of/mrd:name/* return
                <affiliation identifier="{$creator/mrd:member_of/mrd:identifier/text()}" xml:lang="{$label/name()}">{$label/text()}</affiliation>
            }
        </creator>
    }
    </creators>
    <titles>
      {
          for $title in mrd:researchdataset/mrd:title return
            <title xml:lang="{$title/*/name()}">{$title/*/text()}</title>
      }
    </titles>
    <subjects>
      {
          for $keyword in mrd:researchdataset/mrd:keyword/* return
            <subject>{$keyword/text()}</subject>
      }

      {
          for $fs in mrd:researchdataset/mrd:field_of_science/mrd:item/mrd:pref_label/* return
            <subject schemeURI="{$fs/../../mrd:in_scheme/mrd:item[1]/mrd:identifier/text()}"
                     subjectScheme="{$fs/../../mrd:in_scheme/mrd:item[1]/mrd:pref_label/mrd:en/text()}" xml:lang="{$fs/name()}"
                     identifier="{$fs/../../mrd:identifier/text()}"
                     >{$fs/text()}</subject>
      }
      {
          for $fs in mrd:researchdataset/mrd:theme/mrd:item/mrd:pref_label/* return
            <subject schemeURI="{$fs/../../mrd:in_scheme/mrd:item[1]/mrd:identifier/text()}"
                     subjectScheme="{$fs/../../mrd:in_scheme/mrd:item[1]/mrd:pref_label/mrd:en/text()}" xml:lang="{$fs/name()}"
                     identifier="{$fs/../../mrd:identifier/text()}"
                     >{$fs/text()}</subject>
      }
    </subjects>
    <sizes>
      <size>{ mrd:researchdataset/mrd:total_ida_byte_size/text() } bytes</size>
    </sizes>
    <descriptions>
      {
        for $desc in mrd:researchdataset/mrd:description/mrd:item/* return
          <description xml:lang="{$desc/name()}" descriptionType="Abstract">{$desc/text()}</description>
      }
    </descriptions>
    <rightsList>
      {
        for $r in mrd:researchdataset/mrd:access_rights/mrd:license/mrd:item/mrd:title/* return
          <rights xml:lang="{$r/name()}" rightsURI="{$r/../../mrd:license/text()}">{$r/text()}</rights>
      }

    </rightsList>
    <contributors>
      {
          for $c in mrd:researchdataset/mrd:contributor/mrd:item return
            <contributor contributorType="{$c/mrd:contributor_role/mrd:identifier/text()}">
                <contributorName>{$c/mrd:name/text()}</contributorName>
                <nameIdentifier>{$c/mrd:identifier/text()}</nameIdentifier>

                {
                  for $label in $c/mrd:member_of/mrd:name/* return
                    <affiliation identifier="{$c/mrd:member_of/mrd:identifier/text()}" xml:lang="{$label/name()}">{$label/text()}</affiliation>
                }

            </contributor>
      }
      {
        for $c in mrd:researchdataset/mrd:rights_holder/mrd:item return
          <contributor contributorType="RightsHolder">
            {
              for $label in $c/mrd:name/* return
                <contributorName xml:lang="{$label/name()}">{$label/text()}</contributorName>

            }
            <nameIdentifier nameIdentifierScheme="URI">
               {$c/mrd:identifier/text()}
            </nameIdentifier>
          </contributor>

      }
    </contributors>
    <language>{substring-after(mrd:researchdataset/mrd:language[1]/mrd:item/mrd:identifier/text(), 'http://lexvo.org/id/iso639-3/')}</language>

    {
      for $pub in mrd:researchdataset/mrd:publisher/mrd:name/* return
        <publisher identifier="{$pub/../../mrd:identifier/text()}" xml:lang="{$pub/name()}">{$pub/text()}</publisher>
    }
    <geoLocations>
    {
      for $geo in mrd:researchdataset/mrd:spatial/mrd:item return
        <geoLocation>
          <geoLocationPlace>{$geo/mrd:geographic_name/text()}</geoLocationPlace>
          {
            for $wkt in $geo/mrd:as_wkt/mrd:item return
              if(starts-with($wkt/text(), 'POINT')) then (
                <geoLocationPoint>
                  <pointLongitude>{substring-before(substring-after($wkt/text(), 'POINT('), ' ')}</pointLongitude>
                  <pointLatitude>{substring-before(substring-after($wkt/text(), ' '), ')')}</pointLatitude>
                </geoLocationPoint>

              )
              else (
                if(starts-with($wkt/text(), 'POLYGON')) then (
                  <geoLocationPolygon>
                    {for $point in tokenize(substring-before(substring-after($wkt/text(), 'POLYGON(('), '))'), ',') return
                      <polygonPoint>
                        <pointLongitude>{substring-before(normalize-space($point), ' ')}</pointLongitude>
                        <pointLatitude>{substring-after(normalize-space($point), ' ')}</pointLatitude>
                      </polygonPoint>
                    }
                  </geoLocationPolygon>
                )
                else(
                )
              )
          }
        </geoLocation>
    }
    </geoLocations>
</resource>
