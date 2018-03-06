<resource xsi:schemaLocation="http://datacite.org/schema/kernel-4.1 http://schema.datacite.org/meta/kernel-4.1/metadata.xsd" xmlns="http://datacite.org/schema/kernel-4.1" xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance" xmlns:mrd="http://uri.suomi.fi/datamodel/ns/mrd#">
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
        </creator>
    }
    </creators>
</resource>
