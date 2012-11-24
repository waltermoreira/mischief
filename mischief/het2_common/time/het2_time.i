/* 
 * Create type definitions for the common pointer types 
 */

%module het2_time

%include "cpointer.i"
%include "carrays.i"
%include "std_vector.i"
%include "std_map.i"
%include "std_string.i"

%pointer_class(int, intp);
%pointer_class(short, shortp);
%pointer_class(long, longp);
%pointer_class(unsigned int, unsigned_intp);
%pointer_class(unsigned short, unsigned_shortp);
%pointer_class(unsigned long, unsigned_longp);
%pointer_class(unsigned char, unsigned_charp);
%pointer_class(bool, boolp);
%pointer_class(float, floatp);
%pointer_class(double, doublep);

%array_functions(double, dvec);
%array_functions(unsigned short, usvec);
%array_functions(char *,charp );

/*
* this allows the std::string type to be mapped to a string in the interp, not a ptr
*/
%naturalvar std::string;

/*
**   add some typeinfo rules to allow returning of multiple values
**   see: http://swig.org/Doc2.0/Arguments.html#Arguments
*/
//  This rule used in HET2_Time::getIndexTimeFromUTCstr.
//  python calling syntax:
//    success, trajIndexTime = TCSTime.getIndexTimeFromUTCstr("2011-10-27T12:15:14.892880")
//  ...where success is bool, trajIndexTime is double
%apply double *OUTPUT { double *indexTime};

/*
 * Pass the original headers of everything that we are going to wrap
 * to the C compiler.
 */

%{
#include "HET2_Time.h"
%}

/* 
 * Wrap the original headers of the interfaces (both devices and managers)
 */

%include "HET2_Time.h"

%pointer_class(UTC_t, utcp);

