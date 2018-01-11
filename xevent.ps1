#Run in x86, 32bit

#Add-Type -Path 'C:\Program Files (x86)\Microsoft SQL Server\110\Tools\Binn\ManagementStudio\Extensions\Application\Microsoft.SqlServer.XE.Core.dll'
Add-Type -Path 'C:\Program Files (x86)\Microsoft SQL Server\110\Tools\Binn\ManagementStudio\Extensions\Application\Microsoft.SqlServer.XEvent.Linq.dll'
Add-Type -Assembly “system.io.compression.filesystem”

Function iif($If, $Right, $Wrong) {If ($If) {$Right} Else {$Wrong}}

cls
$path = "c:\dump\test\"

$outputfileJson = $path+"xevent_out.json"
$outputfileCsv  = $path+"xevent_out.csv"
$outputfileXml  = $path+"xevent_out.xml"

#Remove-Item $outputfileJson -Recurse
#Remove-Item $outputfileCsv  -Recurse
#Remove-Item $outputfileXml  -Recurse

$time = Get-Date -UFormat "%Y%m%d%H%M%S"

New-Item -path $path$time -ItemType directory | out-null
$source = $path+$time
$destination = $path+"zipped\$time.zip"

$files = @()
$files = gci -Path $path -File -Filter "*.xel" | sort Name | Foreach-Object {$_.Name}

ForEach ($file in $files)
{

$path + $file

$events = New-Object Microsoft.SqlServer.XEvent.Linq.QueryableXEventData($path+$file)

#$events | Foreach-Object { $_.Actions | Where-Object { $_.Name -eq 'client_hostname' } } | Group-Object Value
#$events | ForEach-Object {$_.Action.Value,$_.Fields.Value,$_.Metadata,$_.Name,$_.Package,$_.Timestamp}
$i=0
$xeventCollection=@()
ForEach ($event in $events)
{
    $xevent = New-Object PSObject
    if ($event.Name -ne "login")
       {Add-Member -InputObject $xevent -MemberType NoteProperty -Name event_name -Value "failed login"}
    else
       {Add-Member -InputObject $xevent -MemberType NoteProperty -Name event_name -Value $event.Name}

	Add-Member -InputObject $xevent -MemberType NoteProperty -Name timestamp  -Value $event.Timestamp.LocalDateTime.DateTime

    ForEach ($action in $event.Actions)
    {
        if ( 
        $action.Name -eq "session_server_principal_name" -or
        $action.Name -eq "server_instance_name" -or
        $action.Name -eq "database_name" -or
        $action.Name -eq "database_id" -or
        $action.Name -eq "client_hostname" -or
        $action.Name -eq "client_app_name") 
        {
	      Add-Member -InputObject $xevent -MemberType NoteProperty -Name $action.Name -Value $action.Value
        }
    }
    ForEach ($field in $event.Fields)
    {
        if ($field.Name -eq "options_text" -or $field.Name -eq "message")
        {
        Add-Member -InputObject $xevent -MemberType NoteProperty -Name "remark" -Value $field.Value
        }
    }
    $xeventCollection += $xevent
#   $i += 1
#   if ($i -eq 1000000 ) {break}
}

$xeventCollection | Select-Object server_instance_name,timestamp,event_name,session_server_principal_name,client_hostname,client_app_name,database_name,database_id,remark | ConvertTo-Json | out-file $outputfileJson -Append
$xeventCollection | Select-Object server_instance_name,timestamp,event_name,session_server_principal_name,client_hostname,client_app_name,database_name,database_id,remark | ConvertTo-Xml -Depth 3 -as String | out-file $outputfileXml -Append
$xeventCollection | Select-Object server_instance_name,timestamp,event_name,session_server_principal_name,client_hostname,client_app_name,database_name,database_id, @{l='remark';e={$_.remark -replace "`r`n",";"}} | ConvertTo-Csv -Delimiter "," -NoTypeInformation | out-file $outputfileCsv -Append
$from = $path+$file

Move-Item $from $source  | out-null

}
$level = [System.IO.Compression.CompressionLevel]::Optimal
[io.compression.zipfile]::CreateFromDirectory($source, $destination, $level, $false)
