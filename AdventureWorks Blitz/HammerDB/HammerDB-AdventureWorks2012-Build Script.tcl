#!/usr/local/bin/tclsh8.6
if [catch {package require tclodbc 2.5.1} ] { error "Failed to load tclodbc - ODBC Library Error" }
#EDITABLE OPTIONS##################################################
set total_iterations 1000000;# Number of transactions before logging off
set RAISEERROR "true" ;# Exit script on error (true or false)
set CHECKPOINT "false" ;# Perform SQL Server checkpoint when complete (true or false)
set rampup 0;  # Rampup time in minutes before first Transaction Count is taken
set duration 2;  # Duration in minutes before second Transaction Count is taken
set mode "Local" ;# HammerDB operational mode
set authentication "sql";# Authentication Mode (WINDOWS or SQL)
set server {BUILDBOT\sql2012CS};# Microsoft SQL Server Instance Name
set port "1433";# Microsoft SQL Server Port 
set odbc_driver {SQL Server Native Client 11.0};# ODBC Driver
set uid "sa";#User ID for SQL Server Authentication
set pwd "Password23";#Password for SQL Server Authentication
set database "AdventureWorks2012";# Database Name
#EDITABLE OPTIONS##################################################
#CHECK THREAD STATUS
proc chk_thread {} {
	set chk [package provide Thread]
	if {[string length $chk]} {
	    return "TRUE"
	    } else {
	    return "FALSE"
	}
    }
if { [ chk_thread ] eq "FALSE" } {
error "SQL Server Timed Test Script must be run in Thread Enabled Interpreter"
}

proc connect_string { server port odbc_driver authentication uid pwd } {
if {[ string toupper $authentication ] eq "WINDOWS" } { 
if {[ string match -nocase {*native*} $odbc_driver ] } { 
set connection "DRIVER=$odbc_driver;SERVER=$server;PORT=$port;TRUSTED_CONNECTION=YES"
} else {
set connection "DRIVER=$odbc_driver;SERVER=$server;PORT=$port"
	}
} else {
if {[ string toupper $authentication ] eq "SQL" } {
set connection "DRIVER=$odbc_driver;SERVER=$server;PORT=$port;UID=$uid;PWD=$pwd"
	} else {
puts stderr "Error: neither WINDOWS or SQL Authentication has been specified"
set connection "DRIVER=$odbc_driver;SERVER=$server;PORT=$port"
	}
}
return $connection
}

set mythread [thread::id]
set allthreads [split [thread::names]]
set totalvirtualusers [expr [llength $allthreads] - 1]
set myposition [expr $totalvirtualusers - [lsearch -exact $allthreads $mythread]]
if {![catch {set timeout [tsv::get application timeout]}]} {
if { $timeout eq 0 } { 
set totalvirtualusers [ expr $totalvirtualusers - 1 ] 
set myposition [ expr $myposition - 1 ]
	}
}
switch $myposition {
1 { 
if { $mode eq "Local" || $mode eq "Master" } {
set connection [ connect_string $server $port $odbc_driver $authentication $uid $pwd ]
if [catch {database connect odbc $connection} message ] {
puts stderr "Error: the database connection to $connection could not be established"
error $message
return
} else {
database connect odbc $connection
odbc "use $database"
odbc set autocommit off
}
set ramptime 0
puts "Beginning rampup time of $rampup minutes"
set rampup [ expr $rampup*60000 ]
while {$ramptime != $rampup} {
if { [ tsv::get application abort ] } { break } else { after 6000 }
set ramptime [ expr $ramptime+6000 ]
if { ![ expr {$ramptime % 60000} ] } {
puts "Rampup [ expr $ramptime / 60000 ] minutes complete ..."
	}
}
if { [ tsv::get application abort ] } { break }
puts "Rampup complete, Taking start Transaction Count."
if {[catch {set start_nopm 1  }]} {
puts stderr {error, failed to set start_nopm}
return
}
if {[catch {set start_trans [ odbc "select cntr_value from sys.dm_os_performance_counters where counter_name = 'Batch Requests/sec'" ]}]} {
puts stderr {error, failed to query transaction statistics}
return
} 
puts "Timing test period of $duration in minutes"
set testtime 0
set durmin $duration
set duration [ expr $duration*60000 ]
while {$testtime != $duration} {
if { [ tsv::get application abort ] } { break } else { after 6000 }
set testtime [ expr $testtime+6000 ]
if { ![ expr {$testtime % 60000} ] } {
puts -nonewline  "[ expr $testtime / 60000 ]  ...,"
	}
}
if { [ tsv::get application abort ] } { break }
puts "Test complete, Taking end Transaction Count."
if {[catch {set end_nopm 1 }]} {
puts stderr {error, failed to set end_nopm}
return
}
if {[catch {set end_trans [ odbc "select cntr_value from sys.dm_os_performance_counters where counter_name = 'Batch Requests/sec'" ]}]} {
puts stderr {error, failed to query transaction statistics}
return
} 
if { [ string is integer -strict $end_trans ] && [ string is integer -strict $start_trans ] } {
if { $start_trans < $end_trans }  {
set tpm [ expr {($end_trans - $start_trans)/$durmin} ]
	} else {
puts "Error: SQL Server returned end transaction count data greater than start data"
set tpm 0
	} 
} else {
puts "Error: SQL Server returned non-numeric transaction count data"
set tpm 0
	}
set nopm [ expr {($end_nopm - $start_nopm)/$durmin} ]
puts "$totalvirtualusers Virtual Users configured"
puts "TEST RESULT : System achieved $tpm SQL Server TPM at $nopm NOPM"
tsv::set application abort 1
if { $CHECKPOINT } {
puts "Checkpoint"
if  [catch {odbc "checkpoint"} message ]  {
puts stderr {error, failed to execute checkpoint}
error message
return
	}
puts "Checkpoint Complete"
        }
odbc commit
odbc disconnect
		} else {
puts "Operating in Slave Mode, No Snapshots taken..."
		}
	}
default {
#RANDOM NUMBER
proc RandomNumber {m M} {return [expr {int($m+rand()*($M+1-$m))}]}
#NURand function
proc NURand { iConst x y C } {return [ expr {((([RandomNumber 0 $iConst] | [RandomNumber $x $y]) + $C) % ($y - $x + 1)) + $x }]}
#RANDOM NAME
proc randname { num } {
array set namearr { 0 BAR 1 OUGHT 2 ABLE 3 PRI 4 PRES 5 ESE 6 ANTI 7 CALLY 8 ATION 9 EING }
set name [ concat $namearr([ expr {( $num / 100 ) % 10 }])$namearr([ expr {( $num / 10 ) % 10 }])$namearr([ expr {( $num / 1 ) % 10 }]) ]
return $name
}
#RANDOM City
proc randcity { } {
array set cityarr { 0 London 1 Paris 2 Burien 3 Concord 4 Bellingham 5 Beaverton 6 Berkeley 7 Hamden 8 Wenatchee 9 Mobile }
set citynum  [ RandomNumber 0 9 ]
set name $cityarr($citynum)
return $name
}
#RANDOM STRING
proc randAZazStr {len} { return [subst [string repeat {[format %c [expr {int(rand() * 26) + (int(rand() * 10) > 5 ? 97 : 65)}]]} $len]] }

#TIMESTAMP
proc gettimestamp { } {
set tstamp [ clock format [ clock seconds ] -format "%Y-%m-%d %H:%M:%S" ]
return $tstamp
}

#TIME IN 1997
proc get1997 { } {
set today [clock seconds]
set past [ clock format [expr {$today - 5900 * 60 * 60 * 24}]	-format "%Y-%m-%d %H:%M:%S" ]
return $past
}

# thinktime
proc thinktimems { thinking } {
after $thinking
return
}



#uspGetManagerEmployees
proc uspGetManagerEmployees { uspGetManagerEmployees_st BusinessEntityID RAISEERROR } {

set BusinessEntityID [ RandomNumber 1 200 ]	

if {[ catch {uspGetManagerEmployees_st execute [ list $BusinessEntityID ]} message]} {
if { $RAISEERROR } {
error "Get manager employees : $message"
	} else {
puts $message
} } else {
uspGetManagerEmployees_st fetch op_params
foreach or [array names op_params] {
lappend oput $op_params($or)
}
;
}
odbc commit
}

#SelectPersonByCity
proc SelectPersonByCity { SelectPersonByCity_st City RAISEERROR } {

set City [ randcity ]
	
if {[ catch {SelectPersonByCity_st execute [ list $City ]} message]} {
if { $RAISEERROR } {
error "SelectPersonByCity : $message"
	} else {
puts $message
} } else {
SelectPersonByCity_st fetch op_params
foreach or [array names op_params] {
lappend oput $op_params($or)
}
;
}
odbc commit
}
#CustomerReport
proc CustomerReport { CustomerReport_st City RAISEERROR } {

set City [ randcity ]

if {[ catch {CustomerReport_st execute [ list $City ] } message]} {
if { $RAISEERROR } {
error "CustomerReport : $message"
	} else {
puts $message
} } else {
CustomerReport_st fetch op_params
foreach or [array names op_params] {
lappend oput $op_params($or)
}
;
}
odbc commit
}

#InsertTransactionHistory
proc InsertTransactionHistory { InsertTransactionHistory_st ProductID Quantity ActualCost RAISEERROR } {
set ProductID [ RandomNumber 1 999 ]
set Quantity [ RandomNumber 1 40 ]
set ActualCost [ expr $Quantity * [ RandomNumber 1 16 ] ] 

if {[ catch {InsertTransactionHistory_st execute [ list $ProductID $Quantity $ActualCost ]} message]} {
if { $RAISEERROR } {
error "InsertTransactionHistory : $message"
	} else {
puts $message
} } else {
InsertTransactionHistory_st fetch op_params
foreach or [array names op_params] {
lappend oput $op_params($or)
}
;
}
odbc commit
}

#bou.SelectEmployeeDeptHistoryByShift
proc SelectEmployeeDeptHistoryByShift { SelectEmployeeDeptHistoryByShift_st ShiftID RAISEERROR } {
set ShiftID [ RandomNumber 1 3]

if {[ catch {SelectEmployeeDeptHistoryByShift_st execute [ list $ShiftID ]} message]} {
if { $RAISEERROR } {
error "SelectEmployeeDeptHistoryByShift : $message"
	} else {
puts $message
} } else {
SelectEmployeeDeptHistoryByShift_st fetch op_params
foreach or [array names op_params] {
lappend oput $op_params($or)
}
;
}
odbc commit
}

#bou.SelectTransactionHistoryByProduct
proc SelectTransactionHistoryByProduct { SelectTransactionHistoryByProduct_st ProductID RAISEERROR } {
set ProductID [ RandomNumber 316 999]

if {[ catch {SelectTransactionHistoryByProduct_st execute [ list $ProductID ]} message]} {
if { $RAISEERROR } {
error "SelectTransactionHistoryByProduct : $message"
	} else {
puts $message
} } else {
SelectTransactionHistoryByProduct_st fetch op_params
foreach or [array names op_params] {
lappend oput $op_params($or)
}
;
}
odbc commit
}

#bou.SelectTransactionHistoryByProductAndDate
proc SelectTransactionHistoryByProductAndDate { SelectTransactionHistoryByProductAndDate_st ProductID TransactionDate RAISEERROR } {
set ProductID [ RandomNumber 316 999]
set TransactionDate [ gettimestamp ]

if {[ catch {SelectTransactionHistoryByProductAndDate_st execute [ list $ProductID $TransactionDate ]} message]} {
if { $RAISEERROR } {
error "SelectTransactionHistoryByProductAndDate : $message"
	} else {
puts $message
} } else {
SelectTransactionHistoryByProductAndDate_st fetch op_params
foreach or [array names op_params] {
lappend oput $op_params($or)
}
;
}
odbc commit
}

#bou.SelectTransactionHistoryByDateRange
proc SelectTransactionHistoryByDateRange { SelectTransactionHistoryByDateRange_st TransactionDate RAISEERROR } {
set TransactionDate [ get1997]

if {[ catch {SelectTransactionHistoryByDateRange_st execute [ list $TransactionDate ]} message]} {
if { $RAISEERROR } {
error "SelectTransactionHistoryByDateRange : $message"
	} else {
puts $message
} } else {
SelectTransactionHistoryByDateRange_st fetch op_params
foreach or [array names op_params] {
lappend oput $op_params($or)
}
;
}
odbc commit
}

#UpdateSalesOrderHeader
proc UpdateSalesOrderHeader { UpdateSalesOrderHeader_st CustomerID RAISEERROR } {
set CustomerID [ RandomNumber 11000 30118]

if {[ catch {UpdateSalesOrderHeader_st execute [ list $CustomerID ]} message]} {
if { $RAISEERROR } {
error "UpdateSalesOrderHeader : $message"
	} else {
puts $message
} } else {
UpdateSalesOrderHeader_st fetch op_params
foreach or [array names op_params] {
lappend oput $op_params($or)

}
;
}
odbc commit
}




proc prep_statement { odbc statement_st } {
switch $statement_st {
uspGetManagerEmployees_st {
odbc statement uspGetManagerEmployees_st "EXEC dbo.uspGetManagerEmployees @BusinessEntityID = ?" {INTEGER} 
return uspGetManagerEmployees_st
	}
SelectPersonByCity_st {
odbc statement SelectPersonByCity_st "EXEC bou.SelectPersonByCity @City = ?" {{VARCHAR 256}} 
return SelectPersonByCity_st
	}
CustomerReport_st {
odbc statement CustomerReport_st "EXEC bou.CustomerReport @City = ?" {{VARCHAR 256}} 
return CustomerReport_st
	}	
InsertTransactionHistory_st {
odbc statement InsertTransactionHistory_st "EXEC bou.InsertTransactionHistory @ProductID = ?, @Quantity = ?, @ActualCost = ?" {INTEGER INTEGER INTEGER} 
return InsertTransactionHistory_st
	}
SelectEmployeeDeptHistoryByShift_st {
odbc statement SelectEmployeeDeptHistoryByShift_st "EXEC bou.SelectEmployeeDeptHistoryByShift @ShiftID = ?" {TINYINT} 
return SelectEmployeeDeptHistoryByShift_st
	}
SelectTransactionHistoryByProduct_st {
odbc statement SelectTransactionHistoryByProduct_st "EXEC bou.SelectTransactionHistoryByProduct @ProductID = ?" {INTEGER} 
return SelectTransactionHistoryByProduct_st
	}
SelectTransactionHistoryByProductAndDate_st {
odbc statement SelectTransactionHistoryByProductAndDate_st "EXEC bou.SelectTransactionHistoryByProductAndDate @ProductID = ?, @TransactionDate = ?" {INTEGER TIMESTAMP} 
return SelectTransactionHistoryByProductAndDate_st
	}
SelectTransactionHistoryByDateRange_st {
odbc statement SelectTransactionHistoryByDateRange_st "EXEC bou.SelectTransactionHistoryByDateRange @TransactionDate = ?" {TIMESTAMP} 
return SelectTransactionHistoryByDateRange_st
	}
UpdateSalesOrderHeader_st {
odbc statement UpdateSalesOrderHeader_st "EXEC bou.UpdateSalesOrderHeader @CustomerID=?" {INTEGER} 
return UpdateSalesOrderHeader_st
}
    }
}

#Do the AdventureWorks Hokey Pokey.#

#Initialize all variables
set City [ randcity ]
set BusinessEntityID [ RandomNumber 1 200 ]
set ProductID [ RandomNumber 318 999 ]
set Quantity [ RandomNumber 1 40 ]
set ActualCost [ expr $Quantity * [ RandomNumber 1 16 ] ] 
set ShiftID [ RandomNumber 1 3]
set TransactionDate [ gettimestamp ]
set CustomerID [ RandomNumber 11000 30118]



#Connect to a thing
set connection [ connect_string $server $port $odbc_driver $authentication $uid $pwd ]
if [catch {database connect odbc $connection} message ] {
puts stderr "Error: the database connection to $connection could not be established"
error $message
returned
} else {
database connect odbc $connection
odbc "use $database"
odbc set autocommit off
}
# Prepare all statements
foreach st {
	uspGetManagerEmployees_st 
	SelectPersonByCity_st
	InsertTransactionHistory_st
	CustomerReport_st
	SelectEmployeeDeptHistoryByShift_st
	SelectTransactionHistoryByProduct_st
	SelectTransactionHistoryByProductAndDate_st
	SelectTransactionHistoryByDateRange_st
	UpdateSalesOrderHeader_st
} { set $st [ prep_statement odbc $st ] }


#  Run the things!!! #
puts "Processing $total_iterations transactions without output suppressed..."
for {set it 0} {$it < $total_iterations} {incr it} {
if {  [ tsv::get application abort ]  } { break }
set choice [ RandomNumber 1 100 ]
if {$choice <= 10} {
	thinktimems 5
	UpdateSalesOrderHeader UpdateSalesOrderHeader_st $CustomerID $RAISEERROR
} elseif {$choice <= 50} {
	thinktimems 3
	InsertTransactionHistory InsertTransactionHistory_st $ProductID $Quantity $ActualCost $RAISEERROR
} elseif {$choice <= 50} {
	thinktimems 4
	SelectPersonByCity SelectPersonByCity_st $City $RAISEERROR
#} elseif {$choice <= 27} {
# This has a magnificently high memory grant due to functions in joins
#  Use this with 5+ virtual users if you want RESOURCE_SEMAPHORE
#	CustomerReport CustomerReport_st $City $RAISEERROR
} elseif {$choice <= 60} {
	thinktimems 2
	SelectEmployeeDeptHistoryByShift SelectEmployeeDeptHistoryByShift_st $ShiftID $RAISEERROR
} elseif {$choice <= 70} {
	thinktimems 3
	# This one will be very hard to see in the proc cache
	# Because it gets unique compiles for every literal it's run with!
	# This will get constantly prepared and unprepared.
	# Find with clear trace or by using plan_hash
	set Letter [ RandomNumber 7 79 ]
	set query "SELECT FirstName, isnull(MiddleName,'') as MiddleName, LastName, City FROM Person.Person p 
	JOIN Person.BusinessEntityAddress bea on p.BusinessEntityID=bea.BusinessEntityID 
	join Person.Address a on a.AddressID=bea.AddressID WHERE a.StateProvinceID=$Letter"
	if {[catch {odbc $query} err] } {
		puts "Adhoc Query #1 Error!"
   		puts [format "ERROR is ===\n%s\n===" $err]

   	thinktimems 3
	}
#} elseif {$choice <= 80} {
#	thinktimems 8
#	SelectTransactionHistoryByProduct SelectTransactionHistoryByProduct_st $ProductID $RAISEERROR
} elseif {$choice <= 90} {
	thinktimems 3
	SelectTransactionHistoryByProductAndDate SelectTransactionHistoryByProductAndDate_st $ProductID $TransactionDate $RAISEERROR
} elseif {$choice <= 100} {
	thinktimems 4
	SelectTransactionHistoryByDateRange SelectTransactionHistoryByDateRange_st $TransactionDate $RAISEERROR
}
}
odbc commit


# Unprepare all prepared statements
# If you don't do this, you'll get 
# weird errors/crashes when you cancel or re-run
uspGetManagerEmployees_st drop 
SelectPersonByCity_st drop 
InsertTransactionHistory_st drop 
CustomerReport_st drop 
SelectEmployeeDeptHistoryByShift_st drop
SelectTransactionHistoryByProduct_st drop
SelectTransactionHistoryByProductAndDate_st drop
SelectTransactionHistoryByDateRange_st drop
UpdateSalesOrderHeader_st drop

# buh-bye
odbc disconnect
}
}
  
