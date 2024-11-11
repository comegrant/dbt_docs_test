function Add-RefreshPolicy {
    param (
        [string]$tmdlFilePath,
        [string]$sourceTableName,
        [string]$dateFieldName
    )

    # Les innholdet av TMDL-filen som tekstlinjer
    $tmdlContent = Get-Content -Path $tmdlFilePath

    # Definer refreshPolicy-blokken med det spesifikke kilde-tabellnavnet
    $refreshPolicyBlock = @(
        "`trefreshPolicy"
        "`t`tpolicyType: basic"
        "`t`trollingWindowGranularity: year"
        "`t`trollingWindowPeriods: 6"
        "`t`tincrementalGranularity: month"
        "`t`tincrementalPeriods: 1"
        "`t`tsourceExpression ="
        "`t`t`tlet"
        "`t`t`t`tSource = Databricks.Catalogs(Host, HttpPath, [Catalog=null, Database=null, EnableAutomaticProxyDiscovery=null]),"
        "`t`t`t`tcatalog_Database = Source{[Name=Catalog,Kind=`"Database`"]}[Data],"
        "`t`t`t`tgold_Schema = catalog_Database{[Name=Schema,Kind=`"Schema`"]}[Data],"
        "`t`t`t`t$sourceTableName`_Table = gold_Schema{[Name=`"$sourceTableName`",Kind=`"Table`"]}[Data],"
        "`t`t`t`t`#`"Filtered Rows`" = Table.SelectRows($sourceTableName`_Table, each [$dateFieldName] >= RangeStart and [$dateFieldName] < RangeEnd)"
        "`t`t`tin"
        "`t`t`t`t`#`"Filtered Rows`""
        ""
    )

    $insertIndex = $tmdlContent.IndexOf("")
    # Lag en ny liste som inkluderer refreshPolicy-blokken etter første tomme linje
    $newContent = @()
    for ($i = 0; $i -lt $tmdlContent.Length; $i++) {
        $newContent += $tmdlContent[$i]
        if ($i -eq $insertIndex) {
            $newContent += $refreshPolicyBlock
        }
    }

    # Skriv de oppdaterte linjene tilbake til filen
    $newContent | Set-Content -Path $tmdlFilePath

    Write-Host "Added refreshPolicy block at line 4 in $($tmdlFilePath)"
}