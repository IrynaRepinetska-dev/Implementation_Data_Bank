#!/usr/bin/perl -w

use strict;

my ($rCnt,$formatStr,@formatArray,$i,$outFile,@actVal,$MAX_RANDD,$MAX_RANDI,$MAX_STRLEN,$seed);

undef($rCnt);
undef($formatStr);

while($#ARGV >= 0){
  if($ARGV[0] =~ "-r" && !defined $rCnt && $#ARGV>0){
    shift;
    $rCnt = $ARGV[0];
  }elsif($ARGV[0] =~ "-F" && !defined $formatStr && $#ARGV>0){
    shift;
    $formatStr = $ARGV[0];
  }elsif($ARGV[0] =~ "-s" && !defined $seed && $#ARGV>0){
    shift;
    $seed = $ARGV[0];
  }elsif($ARGV[0] =~ "-o"&& !defined $outFile && $#ARGV>0){
    shift;
    $outFile = $ARGV[0];
    open(OUTFH,">$outFile") or die "Cannot open $outFile: $!";
    select(OUTFH);
  }else{
    printUsage();
  }
  shift;
}

$MAX_RANDD = 100;
$MAX_STRLEN = 30;
$MAX_RANDI = 100;

if(!defined $rCnt){
  $rCnt = 100;
}

if(defined $seed){
  srand($seed);
}

if(!defined $formatStr || !defined $rCnt){
  printUsage();
}

@formatArray = split /:/ , $formatStr;

if($#formatArray >=10){
  printUsage();
}

for($i=0;$i<=$#formatArray;++$i){
  if($formatArray[$i] =~ /i/i){
    $actVal[$i]=int(rand 100);
  }elsif($formatArray[$i] =~ /d/i){
    $actVal[$i]=rand 200;
  }elsif($formatArray[$i] =~ /s/i){
    $actVal[$i]=initString();
  }else{
    printUsage();
  }
}

while($rCnt>0){
  --$rCnt;
  for($i=0;$i<=$#formatArray;++$i){
    if($formatArray[$i] =~ /i/){
      $actVal[$i]=int(rand $MAX_RANDI);
      print "$actVal[$i]\t"
    }elsif($formatArray[$i] =~ /I/){
      ++$actVal[$i];
      print "$actVal[$i]\t"
    }elsif($formatArray[$i] =~ /d/){
      $actVal[$i]=rand $MAX_RANDD;
      print "$actVal[$i]\t"
    }elsif($formatArray[$i] =~ /D/){
      ++$actVal[$i];
      print "$actVal[$i]\t"
    }elsif($formatArray[$i] =~ /s/){
      $actVal[$i] = initString();
      print "\'$actVal[$i]\'\t"
    }elsif($formatArray[$i] =~ /S/){
      $actVal[$i] = nextString($actVal[$i]);
      print "\'$actVal[$i]\'\t"
    }else{
      printUsage();
    }
  }
  print "\n";
}

if(defined $outFile){
  close(OUTFH) or die "Cannot close $outFile: $!";
}

sub nextString
{
  my $str = shift;
  ++$str;
  return $str;
}

sub initString
{
  my $str;
  my $u = int( rand $MAX_STRLEN);
  ++$u;
  for(my $j=0;$j<$u;++$j){
    my $diff =ord('Z') - ord('A') +1;
    if(rand >0.5){
      $str .= chr(ord('A')+int(rand $diff));
    }else{
      $str .= chr(ord('a')+int(rand $diff));
    }
  }
  return $str;
}

sub printUsage{
  die "Usage createTestTuple -r tupleCnt -F formatStr [-s seed] [-o outfile]\n\t formatStr:\n\ti-integer\n\td-double\n\ts-string\n\tUpperCase-unique\n\tz.B.: I:d:I:s\n";
}
