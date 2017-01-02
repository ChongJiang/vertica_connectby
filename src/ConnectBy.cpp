
#include "Vertica.h"
#include <sstream>
#include <map>

using namespace Vertica;
using namespace std;

#define DEFAULT_MAXSIZE 64000
#define DEFAULT_STARTID vint_null
#define DEFAULT_ENDID vint_null
#define DEFAULT_SEPARATOR "/"
#define DEFAULT_SHOWLEVEL true
#define DEFAULT_SHOWPARENTID true
#define DEFAULT_SHOWID true
#define DEFAULT_SHOWNAME true
#define DEFAULT_SHOWNAMEROOT true

class ConnectBy : public TransformFunction
{
    void outputItem(ServerInterface &srvInterface, PartitionWriter &output_writer, 
		    		int columncount, std::string &separator, vint startid, vint endid, vbool showlevel, vbool showparentid, vbool showid, vbool showname, vbool shownameroot,
		    		std::map<vint, vint> &parent, std::map<vint, std::string> *label, std::map<vint, vint> &level, std::map<vint, std::string> &lable_path, vint id){
        bool validate = false;

	    // each column for CONNECT_BY_PATH
	    for(int i=2; i<columncount; i++){
	    	// find label_path, and level
	    	if (lable_path.count(id) == 0) {
	    		vint parent_id = parent[id];
	    		
				if(i==2) {
					srvInterface.log("ConnectBy.outputItem: begin (id, parent_id, parent.count(id), parent.count(parent_id), parent.size(), startid, endid, showlevel, showparentid, showid, showname)=(%lld, %lld, %d, %d, %d, %lld, %lld, %d, %d, %d, %d)", id, parent_id, (int)parent.count(id), (int)parent.count(parent_id), (int)parent.size(), startid, endid, showlevel, showparentid, showid, showname);
				}

	    		if( (endid != vint_null) && (id == endid) || (endid == vint_null) && (label[0].count(parent_id) == 0) ) {
	    			// root item
					if(i==2) {
						level[id] = 1;
						srvInterface.log("ConnectBy.outputItem: output root item (id, parent_id, level, startid, endid)=(%lld, %lld, %lld, %lld, %lld)", id, parent_id, level[id], startid, endid);
					}
			    	lable_path[id] = label[i-2][id];
	    		}
	    		else {
					// output parent item
					if (lable_path.count(parent_id) == 0 ) {
						if(i==2) {
							srvInterface.log("ConnectBy.outputItem: try parent first (id, parent_id, startid, endid)=(%lld, %lld, %lld, %lld)", id, parent_id, startid, endid);
						}
						outputItem(srvInterface, output_writer, 
									columncount, separator, startid, endid, showlevel, showparentid, showid, showname, shownameroot, 
									parent, label, level, lable_path, parent_id);
					}
					
					if (lable_path.count(parent_id) > 0 ) {
	    				// Found the parent's path in the lable_path
	    				// prepend parent + separator + label
						if(i==2) {
	    					level[id] = level[parent_id] + 1ull;
							srvInterface.log("ConnectBy.outputItem: output when parent finished (id, parent_id, level, startid, endid)=(%lld, %lld, %lld, %lld, %lld)", id, parent_id, level[id], startid, endid);
						}
	    				lable_path[id] = lable_path[parent_id] + separator + label[i-2][id];
	    			} 
	    		}
	    	}
	    	
	    	validate = (lable_path.count(id) > 0);
	    	if(validate) {
	    		// find label_root
		    	string label_root;
		    	for(vint pid = id; (endid != vint_null) && (pid != endid) || (endid == vint_null) && (pid != 0); pid = parent[pid]) {
		    		label_root = label[i-2][pid];
		    	}

		    	if(showname){
					output_writer.getStringRef(2*(i-2) + (showlevel?1:0)+(showparentid?1:0)+(showid?1:0)).copy(label[i-2][id]);
				}
		    	if(shownameroot){
					output_writer.getStringRef(2*(i-2) + (showlevel?1:0)+(showparentid?1:0)+(showid?1:0)+(showname?1:0)).copy(label_root);
				}
				output_writer.getStringRef(2*(i-2) + (showlevel?1:0)+(showparentid?1:0)+(showid?1:0)+(showname?1:0)+(shownameroot?1:0)).copy(lable_path[id]);
		    }
		}
		
	    if(validate) {
	    	if(showlevel){
		    	output_writer.setInt(0, level[id]);
			}
	    	if(showparentid){
		    	output_writer.setInt((showlevel?1:0), parent[id]);
			}
	    	if(showid){
		    	output_writer.setInt((showlevel?1:0)+(showparentid?1:0), id);
			}
			
			output_writer.next();
		}
    }
	
    virtual void processPartition(ServerInterface &srvInterface,
                                  PartitionReader &input_reader,
                                  PartitionWriter &output_writer)
    {
    	int columncount = input_reader.getNumCols();
    	if (columncount < 3)
            vt_report_error(0, "Function need 3 argument at least, but %zu provided", columncount);

        ParamReader paramReader = srvInterface.getParamReader();
        vint startid = DEFAULT_STARTID; // default: vint_null(0x8000000000000000LL), meams including all items
        if (paramReader.containsParameter("startid"))
            startid = paramReader.getIntRef("startid");
        vint endid = DEFAULT_ENDID; // default: vint_null(0x8000000000000000LL), meams stopping when parent_id equals 0 or null
        if (paramReader.containsParameter("endid"))
            endid = paramReader.getIntRef("endid");
        vint maxsize = DEFAULT_MAXSIZE;
        if (paramReader.containsParameter("maxsize"))
            maxsize = paramReader.getIntRef("maxsize");
        std::string separator = DEFAULT_SEPARATOR;
        if (paramReader.containsParameter("separator")){
            separator = paramReader.getStringRef("separator").str();
        }
        vbool showlevel = DEFAULT_SHOWLEVEL;
        if (paramReader.containsParameter("showlevel")){
            showlevel = paramReader.getBoolRef("showlevel");
        }
        vbool showparentid = DEFAULT_SHOWPARENTID;
        if (paramReader.containsParameter("showparentid")){
            showparentid = paramReader.getBoolRef("showparentid");
        }
        vbool showid = DEFAULT_SHOWID;
        if (paramReader.containsParameter("showid")){
            showid = paramReader.getBoolRef("showid");
        }
        vbool showname = DEFAULT_SHOWNAME;
        if (paramReader.containsParameter("showname")){
            showname = paramReader.getBoolRef("showname");
        }
        vbool shownameroot = DEFAULT_SHOWNAMEROOT;
        if (paramReader.containsParameter("shownameroot")){
            shownameroot = paramReader.getBoolRef("shownameroot");
        }


    	std::map<vint, vint> parent;  // parent_id
    	std::map<vint, std::string> label[columncount-2]; // columns for CONNECT_BY_PATH
        do {
            vint id = input_reader.getIntRef(1);
            parent[id] = input_reader.getIntRef(0);
            for(int i=2; i<columncount; i++){
            	label[i-2][id] = input_reader.getStringRef(i).str();
            }
        } while (input_reader.next());

        std::map<vint, vint> level;
        std::map<vint, std::string> lable_path;
        
        std::map<vint, vint>::iterator p;
 
       // move to startid item
		srvInterface.log("ConnectBy.processPartition: before connect (startid,endid)=(%lld,%lld)" , startid, endid);
        for (p = parent.begin(); (startid != vint_null) && (p->first != startid) && (p != parent.end()); ++p) {
			srvInterface.log("ConnectBy.processPartition: move to startid item (id, startid, endid)=(%lld,%lld,%lld)" , p->first, startid, endid);
        }
        
        // connect
        for (; p != parent.end(); ++p) {
        	vint id = p->first;

 			outputItem(srvInterface, output_writer, 
		 				columncount, separator, startid, endid, showlevel, showparentid, showid, showname, shownameroot, 
		 				parent, label, level, lable_path, id);
 			// only one startid item if specified
 			if((startid != vint_null)) break;
         }
    }
};

class ConnectByFactory : public TransformFunctionFactory
{
    virtual void getPrototype(ServerInterface &srvInterface, ColumnTypes &argTypes, ColumnTypes &returnType)
    {
        // get parameters
        ParamReader paramReader = srvInterface.getParamReader();

        argTypes.addAny();
        
        // Note: need not add any type to returnType. empty returnType means any columns and types!
        // int columncount = 1;
        // if (paramReader.containsParameter("columncount"))
        //     columncount = paramReader.getIntRef("columncount");
        // 
        // for(int i=0; i<columncount; i++) {
        //     returnType.addVarchar();
        // }
    }

    virtual void getReturnType(ServerInterface &srvInterface,
                               const SizedColumnTypes &input_types,
                               SizedColumnTypes &output_types)
    {
    	int columncount = input_types.getColumnCount();
    	if (columncount < 3)
            vt_report_error(0, "Function need 3 argument at least, but %zu provided", columncount);

        // get parameters
        ParamReader paramReader = srvInterface.getParamReader();
        vint maxsize = DEFAULT_MAXSIZE;
        if (paramReader.containsParameter("maxsize"))
            maxsize = paramReader.getIntRef("maxsize");
        std::string separator = DEFAULT_SEPARATOR;
        if (paramReader.containsParameter("separator")){
            separator = paramReader.getStringRef("separator").str();
        }
        vbool showlevel = DEFAULT_SHOWLEVEL;
        if (paramReader.containsParameter("showlevel")){
            showlevel = paramReader.getBoolRef("showlevel");
        }
        vbool showparentid = DEFAULT_SHOWPARENTID;
        if (paramReader.containsParameter("showparentid")){
            showparentid = paramReader.getBoolRef("showparentid");
        }
        vbool showid = DEFAULT_SHOWID;
        if (paramReader.containsParameter("showid")){
            showid = paramReader.getBoolRef("showid");
        }
        vbool showname = DEFAULT_SHOWNAME;
        if (paramReader.containsParameter("showname")){
            showname = paramReader.getBoolRef("showname");
        }
        vbool shownameroot = DEFAULT_SHOWNAMEROOT;
        if (paramReader.containsParameter("shownameroot")){
            shownameroot = paramReader.getBoolRef("shownameroot");
        }

        // output can be wide.  Include extra space for a last ", ..."
        vint resultsize = maxsize + separator.length() + 3;
        char resultColumnName[256];
        if(showlevel)
       		output_types.addInt("level"); // for level column
        if(showparentid)
        	output_types.addInt(input_types.getColumnName(0).c_str()); // for parentid column
        if(showid)
        	output_types.addInt(input_types.getColumnName(1).c_str()); // for id column
        for(int i=2; i<columncount; i++) {
	        if(showname)
	            output_types.addVarchar(input_types.getColumnType(i).getStringLength(), input_types.getColumnName(i).c_str());
	        if(shownameroot) {
	            sprintf(resultColumnName, "%s_root", input_types.getColumnName(i).c_str());
	            output_types.addVarchar(resultsize, resultColumnName);
	        }
            sprintf(resultColumnName, "%s_path", input_types.getColumnName(i).c_str());
            output_types.addVarchar(resultsize, resultColumnName);
        }
    }

    // Defines the parameters for this UDSF. Works similarly to defining
    // arguments and return types.
    virtual void getParameterType(ServerInterface &srvInterface,
                                  SizedColumnTypes &parameterTypes) {
        //parameter: startid: id of the most bottom item. Optional paramter, default value is vint_null(0x8000000000000000LL), meams including all items.
        parameterTypes.addInt("startid");
        //parameter: endid: id of the most top item. Optional paramter, default value is vint_null(0x8000000000000000LL), meams stopping when parent_id equals 0 or null.
        parameterTypes.addInt("endid");
        //parameter: maximum output size, default value is 64000.
        parameterTypes.addInt("maxsize");
        //parameter: separator string for concatenating, default value is ', '.
        parameterTypes.addVarchar(200, "separator");
		//parameter: showlevel: switch for display level colum. Optional paramter, default true. 
        parameterTypes.addBool("showlevel");
		//parameter: showid: switch for display id colum. Optional paramter, default true. 
        parameterTypes.addBool("showid");
		//parameter: showparentid: switch for display showparentid colum. Optional paramter, default true. 
        parameterTypes.addBool("showparentid");
		//parameter: showname: switch for display name colum. Optional paramter, default true. 
        parameterTypes.addBool("showname");
		//parameter: shownameroot: switch for display root of name colum. Optional paramter, default true. 
        parameterTypes.addBool("shownameroot");
    }


    virtual TransformFunction *createTransformFunction(ServerInterface &srvInterface)
    { return vt_createFuncObj(srvInterface.allocator, ConnectBy); }

};

RegisterFactory(ConnectByFactory);
