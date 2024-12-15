from pyspark.sql import functions as F
from pyspark.sql.window import Window
from src.config import Config
import logging as log
from src.utils import write_output
import json

class CrashAnalysis:
    def __init__(self, df_Units, df_Primary_Person, df_damages, df_charges):
        self.df_Units = df_Units
        self.df_Primary_Person = df_Primary_Person
        self.df_damages = df_damages
        self.df_charges = df_charges
        self.config = Config()

    def count_male_deaths_gt(self):
        """Analysis1: Counts crashes where more than 2 males were killed."""
        df_male = self.df_Primary_Person.filter(
            (self.df_Primary_Person.PRSN_GNDR_ID == 'Male') & 
            (self.df_Primary_Person.PRSN_DEATH_TIME.isNotNull())
        )
        grp = df_male.groupBy("CRASH_ID").agg(F.count("PRSN_NBR").alias("male_count"))
        fil_res = grp.filter(grp["male_count"] > 2)
        res = fil_res.count()
        return f'Number of Male Death > 2:   {res}'

    def count_two_wheelers(self):
        """Analysis2: Counts crashes involving two-wheelers."""
        two_wheelers = self.df_Units.filter(F.col("VEH_BODY_STYL_ID").rlike("MOTORCYCLE|POLICE MOTORCYCLE"))
        two_wheelers_count = two_wheelers.select("CRASH_ID").distinct().count()
        return f'Two Wheelers Booked for crashes: {two_wheelers_count}'

    def top_5_vehicle_makes(self):
        """Analysis3: Finds top 5 vehicle makes where driver died and airbags didn't deploy."""
        driver_death_no_airbag = self.df_Primary_Person.filter(
            (self.df_Primary_Person.PRSN_INJRY_SEV_ID == 'KILLED') & 
            (self.df_Primary_Person.PRSN_AIRBAG_ID == 'NOT DEPLOYED')
        )
        joined_df = driver_death_no_airbag.join(self.df_Units, on="CRASH_ID", how="inner")
        valid_vehicle_data = joined_df.filter(
            (joined_df.VEH_MAKE_ID.isNotNull()) &
            (joined_df.VEH_MAKE_ID != 'NA')
        )
        vehicle_make_counts = valid_vehicle_data.groupBy("VEH_MAKE_ID").count()
        top_5_vehicles = vehicle_make_counts.orderBy(F.desc("count")).limit(5)
        return f'Top 5 Vehicles where driver died and Airbag didnot open: {top_5_vehicles.collect()}'

    def valid_lic_hit_and_run(self):
        """Analysis4: Number of Vehicles with driver having 
                        valid licences involved in hit and run"""
        hit_run_vehicles = self.df_Units.filter(self.df_Units.VEH_HNR_FL == 'Y')
        valid_licenses = self.df_Primary_Person.filter(self.df_Primary_Person.DRVR_LIC_TYPE_ID.isin(
                ["DRIVER LICENSE", "COMMERCIAL DRIVER LIC."]))
        joined_df = hit_run_vehicles.join(valid_licenses, on="CRASH_ID", how="inner")
        vehicle_count = joined_df.select("VEH_LIC_STATE_ID", "VIN").distinct().count()
        return f'Number of vehicles with valid licenses involved in hit and run: {vehicle_count}'

    def max_acc_no_females(self):
        """Analysis5: State has highest number of accidents in which females are not involved"""
        result = (
            self.df_Primary_Person.filter(self.df_Primary_Person.PRSN_GNDR_ID != 'FEMALE')
            .groupBy("DRVR_LIC_STATE_ID")
            .agg(F.count("CRASH_ID").alias("acc_count"))
            .orderBy(F.desc("acc_count"))
            .first()
        )
        return f'State: {result["DRVR_LIC_STATE_ID"]}'

    def top_3_to_5_vehicle_makes(self):
        injury_columns = [
            "INCAP_INJRY_CNT", "NONINCAP_INJRY_CNT", "POSS_INJRY_CNT", "UNKN_INJRY_CNT","DEATH_CNT"
        ]
        df_injury_count = self.df_Units.withColumn(
            "TOTAL_INJURIES", 
            sum([F.col(c) for c in injury_columns])
        )
        df_injury_count = df_injury_count.groupby("VEH_MAKE_ID").agg(
            F.sum("TOTAL_INJURIES").alias("TOTAL_INJURIES")
        )
            # Add a row number to order the makes based on injuries
        window_spec = Window.orderBy(F.desc("TOTAL_INJURIES"))
        df_with_row_number = df_injury_count.withColumn("row_num", F.row_number().over(window_spec))
        
        # Filter for the 3rd to 5th top vehicle makes
        df_filtered = df_with_row_number.filter((F.col("row_num") >= 3) & (F.col("row_num") <= 5))
        df_filtered = df_filtered.drop('row_num')

        return f'Top 3rd to 5th vechile contributed in largest injuries: {df_filtered.collect()}'

    def top_ethnic_group_per_body_style(self):
        joined_df = self.df_Units.join(self.df_Primary_Person, on="CRASH_ID", how="inner")
        grouped_df = (
            joined_df.groupBy("VEH_BODY_STYL_ID", "PRSN_ETHNICITY_ID")
            .agg(F.count("*").alias("count"))
        )
        window = Window.partitionBy("VEH_BODY_STYL_ID").orderBy(F.desc("count"))
        ranked_df = grouped_df.withColumn("rank", F.row_number().over(window))
        top_ethnic_groups = ranked_df.filter(F.col("rank") == 1).select(
            "VEH_BODY_STYL_ID", "PRSN_ETHNICITY_ID"
        )
        return f'Top ethnic user group unique body style: {top_ethnic_groups.collect()}'

    def top_zip_codes_alcohol_crashes(self):
        alcohol_crashes = self.df_Units.filter(
            (F.col("CONTRIB_FACTR_1_ID").like("%ALCOHOL%")) |
            (F.col("CONTRIB_FACTR_2_ID").like("%ALCOHOL%"))
        )
        alcohol_zip_crashes = alcohol_crashes.join(
            self.df_Primary_Person.select("CRASH_ID", "DRVR_ZIP"),
            on="CRASH_ID",
            how="inner"
        )
        alcohol_zip_crashes = alcohol_zip_crashes.filter(F.col("DRVR_ZIP").isNotNull())
        zip_crash_counts = (
            alcohol_zip_crashes.groupBy("DRVR_ZIP")
            .agg(F.count("*").alias("crash_count"))
            .orderBy(F.desc("crash_count"))
        )
        top_5_zip_codes = zip_crash_counts.limit(5)
        return f'Top 5 zip code with highest number of crashes: {top_5_zip_codes.collect()}'

    def count_crash_ids_no_damage(self):
        # Join the damages DataFrame with the units DataFrame on CRASH_ID
        df = (
            self.df_damages.join(self.df_Units, on=["CRASH_ID"], how="inner")
            .filter(
                ((self.df_Units.VEH_DMAG_SCL_1_ID.isin(["DAMAGED 5","DAMAGED 6", "DAMAGED 7 HIGHEST"]))
                | ((self.df_Units.VEH_DMAG_SCL_2_ID.isin(["DAMAGED 5","DAMAGED 6", "DAMAGED 7 HIGHEST"]))
            )))
            .filter(self.df_damages.DAMAGED_PROPERTY.isin(["NO DAMAGES TO THE CITY POLE 214385","GUARD RAIL (POSSIBLE CONTACT-NO DAMAGE)",
                                                    "LIGHT POLE- NO DAMAGE","NO DAMAGE, SIGNAL LIGHT POLE","CONCRETE BARRIER-NO DAMAGE",
                                                    "NO DAMAGE TO BARRICADE","CEMENT RETAINING WALL-NO DAMAGE","CONCRETE BARRIER-NO DAMAGE",
                                                    "CONCRETE MEDIAN (NO DAMAGE)","NO DAMAGE TO FENCE"])) # No damaged property
            .filter(~self.df_Units.FIN_RESP_TYPE_ID.isin(["NA","SURETY BOND"])) # Insurance is proof of liability
        )

        # Count distinct Crash IDs
        crash_id = df.select("CRASH_ID").distinct().collect()
        return f'Distinct Crash IDs meeting the criteria: {crash_id}'

    def top_5_vehicle_makes_driver(self):
        top_25_state_list = [
            row[0]
            for row in self.df_Units.filter(
                F.col("VEH_LIC_STATE_ID").cast("int").isNull()
            )
            .groupBy("VEH_LIC_STATE_ID")
            .count()
            .orderBy(F.col("count").desc())
            .limit(25)
            .collect()
        ]
        top_10_used_vehicle_colors = [
            row[0]
            for row in self.df_Units.filter(self.df_Units.VEH_COLOR_ID != "NA")
            .groupBy("VEH_COLOR_ID")
            .count()
            .orderBy(F.col("count").desc())
            .limit(10)
            .collect()
        ]
        df = (
            self.df_charges.join(self.df_Primary_Person, on=["CRASH_ID"], how="inner")
            .join(self.df_Units, on=["CRASH_ID"], how="inner")
            .filter(self.df_charges.CHARGE.contains("SPEED"))
            .filter(self.df_Primary_Person.DRVR_LIC_TYPE_ID.isin(["DRIVER LICENSE", "COMMERCIAL DRIVER LIC."]))
            .filter(self.df_Units.VEH_COLOR_ID.isin(top_10_used_vehicle_colors))
            .filter(self.df_Units.VEH_LIC_STATE_ID.isin(top_25_state_list))
            .groupBy("VEH_MAKE_ID")
            .count()
            .orderBy(F.col("count").desc())
            .limit(5)
        )
        return f'Top 5 vehichle makes driver  are charged with speeding offences: {df.collect()}'

    def write_output(self, output_file):
        """Writes the results to the specified output file."""
        log.info(f"Results written to {output_file}")
        write_output(self.count_male_deaths_gt(), output_file, 'a')
        write_output(self.count_two_wheelers(), output_file, 'a')
        write_output(self.top_5_vehicle_makes(), output_file, 'a')
        write_output(self.valid_lic_hit_and_run(), output_file, 'a')
        write_output(self.max_acc_no_females(), output_file, 'a')
        write_output(self.top_3_to_5_vehicle_makes(), output_file, 'a')
        write_output(self.top_ethnic_group_per_body_style(), output_file, 'a')
        write_output(self.top_zip_codes_alcohol_crashes(), output_file, 'a')
        write_output(self.count_crash_ids_no_damage(), output_file, 'a')
        write_output(self.top_5_vehicle_makes_driver(), output_file, 'a')
        