/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package marquez.common.models;

import static org.assertj.core.api.AssertionsForClassTypes.assertThat;
import static org.assertj.core.api.AssertionsForClassTypes.assertThatThrownBy;

import marquez.UnitTests;
import org.junit.Test;
import org.junit.experimental.categories.Category;

@Category(UnitTests.class)
public class JobTypeTest {
  @Test
  public void testDefaultValue() {
    assertThat(JobType.fromString(null)).isEqualTo(JobType.BATCH);
  }

  @Test
  public void testInvalidValue() {
    assertThatThrownBy(() -> JobType.valueOf("INVALID"))
        .isInstanceOf(IllegalArgumentException.class);
  }

  @Test
  public void testValidJobType() {
    assertThat(JobType.isValid("SERVICE")).isTrue();
    assertThat(JobType.isValid("STREAM")).isTrue();
    assertThat(JobType.isValid("BATCH")).isTrue();
  }

  @Test
  public void testInvalidJobType() {
    assertThat(JobType.isValid("NOSUCHSERVICE")).isFalse();
  }

  @Test
  public void testNullJobType() {
    assertThat(JobType.isValid(null)).isFalse();
  }
}
