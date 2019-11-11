/*
 * https://software.intel.com/en-us/vtune-amplifier-help-instrumentation-and-tracing-technology-api-reference
*/
#ifndef TRACING_VTUNE_H
#define TRACING_VTUNE_H

#include <string>
#include <ittnotify.h>
#include <string.h>
#include <mutex>

namespace vtune_tracing {

/*
 * Thread Naming
 */

	void ignore_thread(void){
		#if USE_VTUNE
			__itt_thread_ignore ();
		#endif
	}

	void set_name(std::string thread_name) {
		#if USE_VTUNE
			__itt_thread_set_name (thread_name.c_str());
		#endif
	}

	void set_name(const char *thread_name) {
		#if USE_VTUNE
			__itt_thread_set_name (thread_name);
		#endif
	}

/*
 * Task
 */

class VTuneDomain {

	private:
		__itt_domain* domain=NULL;
		char *name="NULL\0";
		std::mutex domain_m;
		std::mutex name_m;

	public:
		VTuneDomain()
		{
			name = strdup("None\0");
		}

		VTuneDomain(char *domain_name)
		{
			name = strdup(domain_name);
			domain = __itt_domain_create(name);
    		domain->flags = 1;
		}

		VTuneDomain(VTuneDomain *d)
		{
			name = strdup("None2\0");
			domain=d->getDomain();
		}

		__itt_domain* getDomain()
		{
			return domain;
		}

		char* getName()
		{
			char *new_name=NULL;
			if(name)
				new_name = strdup(name);
			return new_name;
		}

		void start_task(char *task_name)
		{
			#if USE_VTUNE
			domain_m.lock();
			if(domain){
				__itt_task_begin (domain,__itt_null, __itt_null,__itt_string_handle_create(task_name));
			}
			domain_m.unlock();
			#endif
		}

		void end_task(char *name_)
		{
			#if USE_VTUNE
			domain_m.lock();
				if(domain!=NULL)
					__itt_task_end(domain);
			domain_m.unlock();
			#endif
		}

		void end_task()
		{
			#if USE_VTUNE
			domain_m.lock();
				if(domain!=NULL)
					__itt_task_end(domain);
			domain_m.unlock();
			#endif
		}

		void set_name()
		{
			#if USE_VTUNE
			const char* _name = strdup(name);
			if(name!=NULL)
				__itt_thread_set_name(_name);
			else
				__itt_thread_set_name("None");
			#endif
		}

		void start_frame()
		{
			#if USE_VTUNE
			if(domain!=NULL)
				__itt_frame_begin_v3(domain,NULL);
			#endif
		}

		void end_frame()
		{
			#if USE_VTUNE
			if(domain!=NULL)
				__itt_frame_end_v3(domain,NULL);
			#endif
		}
};

}

#endif