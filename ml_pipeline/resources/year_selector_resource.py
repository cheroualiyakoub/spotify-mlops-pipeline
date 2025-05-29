from dagster import resource, Field

{
"resources":
  {
    "year_selector":
    {
        "config":
          {
            "selected_years":['2000', '2001']
          }
    }  
  }
}

@resource(
    config_schema={
        "selected_years": Field(
            [str], 
            is_required=False,
            description="List of years to process (e.g., ['2000', '2001']). If None, will process all available years."
        )
    }
)
def year_selector(context):
    return YearSelector(context.resource_config.get("selected_years"))


class YearSelector:
    """Helper class for selecting which years to process from LakeFS development branch"""
    
    def __init__(self, selected_years=None):
        self.selected_years = selected_years
    
    def get_available_years(self, context):
        """Discover available years in LakeFS development branch"""
        try:
            # Use the LakeFS client to list objects
            response = context.resources.lakefs_client.objects.list_objects(
                repository="spotify-repo",
                ref="development",
                prefix="year="
            )
            
            # Extract years from paths
            years = set()
            for obj in response.results:
                # Expected format: year=YYYY/data.csv
                path = obj.path
                if 'year=' in path:
                    year = path.split('year=')[1].split('/')[0]
                    years.add(year)
            
            years_list = sorted(list(years))
            context.log.info(f"Found available years in LakeFS: {', '.join(years_list)}")
            return years_list
        
        except Exception as e:
            context.log.error(f"Failed to get available years from LakeFS: {e}")
            raise
    
    def get_selected_years(self, context):
        """Get the years that should be processed based on configuration"""
        available_years = self.get_available_years(context)
        
        if not self.selected_years:
            # If no years specified, use all available years
            context.log.info(f"No specific years configured, using all {len(available_years)} available years")
            return available_years
        
        # Return the configured years
        context.log.info(f"Using configured years: {', '.join(self.selected_years)}")
        return self.selected_years
    
    def validate_selection(self, context):
        """Validate that the selected years exist and return valid years"""
        available_years = set(self.get_available_years(context))
        
        if not self.selected_years:
            # If no specific selection, all available years are valid
            return sorted(list(available_years))
        
        # Check which selected years are actually available
        valid_years = []
        invalid_years = []
        
        for year in self.selected_years:
            if year in available_years:
                valid_years.append(year)
            else:
                invalid_years.append(year)
        
        # Log warnings for invalid years
        if invalid_years:
            context.log.warning(
                f"Invalid years selected: {', '.join(invalid_years)}. These years do not exist in LakeFS."
            )
        
        if not valid_years:
            error_msg = f"None of the selected years {self.selected_years} are available in LakeFS!"
            context.log.error(error_msg)
            raise ValueError(error_msg)
        
        context.log.info(f"Validated years to process: {', '.join(valid_years)}")
        return valid_years