<resource>
    <identifier>{ ResearchDataset/preferred_identifier/text() }</identifier>
    <alternateIdentifier alternateIdentifierType="URN">
        { ResearchDataset/urn_identifier/text() }
    </alternateIdentifier>
    <alternateIdentifier alternateIdentifierType="preferred_identifier">
        { ResearchDataset/preferred_identifier/text() }
    </alternateIdentifier>
    <dates>
        <date dateType="Updated">{ ResearchDataset/modified/text() }</date>
        <publicationYear>{ substring(ResearchDataset/modified/text(), 0 , 5) }</publicationYear>
        <date dateType="Issued">{ ResearchDataset/issued/text() }</date>
    </dates>
    <version>{ ResearchDataset/version_info/text() }</version>
    <!--<titles>
    {
        for $keyword in ResearchDataset/title/ return 
        <title xml:lang="{ResearchDataset/title/()}">
            { ResearchDataset/title/element()/text() }
        </title>
    }
    </titles>
    <subjects>
    {
        for $keyword in ResearchDataset/keyword/item return 
        <subject xml:lang="XX" schemeURI="XX">string</subject>
         
        <subject xml:lang="XX">{ ResearchDataset/description/name() }</subject>
         
    {
    </subjects>-->
    <creators>
    {
        for $creator in ResearchDataset/creator/item return 
        <creator>
            <creatorName>{ $creator/name/text() }</creatorName>
        </creator>
    }
    </creators>
</resource>