<?php
declare(strict_types=1);

namespace App\Model\Entity;

use Cake\ORM\Entity;

/**
 * RelBenchmarksClasseFundo Entity
 *
 * @property int $tipo_benchmark_id
 * @property int $tipo_classe_fundo_id
 *
 * @property \App\Model\Entity\TipoBenchmark $tipo_benchmark
 * @property \App\Model\Entity\TipoClasseFundo $tipo_classe_fundo
 */
class RelBenchmarksClasseFundo extends Entity
{
    /**
     * Fields that can be mass assigned using newEntity() or patchEntity().
     *
     * Note that when '*' is set to true, this allows all unspecified fields to
     * be mass assigned. For security purposes, it is advised to set '*' to false
     * (or remove it), and explicitly make individual fields accessible as needed.
     *
     * @var array
     */
    protected $_accessible = [
        'tipo_benchmark' => true,
        'tipo_classe_fundo' => true,
    ];
}
