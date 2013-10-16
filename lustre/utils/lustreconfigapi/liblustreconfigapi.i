%module lustreconfigapi

%{
#include "liblustreconfigapi.h"
%}

%typemap(in) FILE * {
        $1 = PyFile_AsFile($input);
}

%typemap(in, numinputs=0) cYAML** (cYAML *temp) {
        temp = NULL;
        $1 = &temp;
}

%typemap(argout) cYAML** {
        /* The purpose of this typemap is to be able to handle out params
           Ex: if the function being called is: foo(cYAML**a, cYAML **b)
           then from python you'd call it: o1, o2 = foo()*/
        PyObject *o, *o2, *o3;
        o = SWIG_NewPointerObj(SWIG_as_voidptr(*$1), $*1_descriptor, SWIG_POINTER_OWN);
        if ((!$result) || ($result == Py_None))
                $result = o;
        else
        {
                if(!PyTuple_Check($result))
                {
                        /* insert the original result in the tuple */
                        o2 = $result;
                        $result = PyTuple_New(1);
                        PyTuple_SetItem($result, 0, o2);
                }
                o3 = PyTuple_New(1);
                PyTuple_SetItem(o3, 0, o);
                o2 = $result;
                $result = PySequence_Concat(o2, o3);
                Py_DECREF(o2);
                Py_DECREF(o3);
        }
}


#include "liblustreconfigapi.h"


