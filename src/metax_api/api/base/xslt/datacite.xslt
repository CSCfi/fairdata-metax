<resource>
    <identifier>{ researchdataset/preferred_identifier/text() }</identifier>
    <alternateIdentifier alternateIdentifierType="URN">
        { researchdataset/urn_identifier/text() }
    </alternateIdentifier>
    <alternateIdentifier alternateIdentifierType="preferred_identifier">
        { researchdataset/preferred_identifier/text() }
    </alternateIdentifier>
    <dates>
        <date dateType="Updated">{ researchdataset/modified/text() }</date>
        <publicationYear>{ substring(researchdataset/modified/text(), 0 , 5) }</publicationYear>
        <date dateType="Issued">{ researchdataset/issued/text() }</date>
    </dates>
    <version>{ researchdataset/version_info/text() }</version>
    <creators>
    {
        for $creator in researchdataset/creator/item return 
        <creator>
            <creatorName>{ $creator/name/text() }</creatorName>
        </creator>
    }
    </creators>
</resource>