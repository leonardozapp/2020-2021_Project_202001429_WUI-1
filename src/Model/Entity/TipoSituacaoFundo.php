<?php
declare(strict_types=1);

namespace App\Model\Entity;

use Cake\ORM\Entity;

/**
 * TipoSituacaoFundo Entity
 *
 * @property int $id
 * @property string $situacao
 *
 * @property \App\Model\Entity\SituacaoFundo[] $situacao_fundos
 */
class TipoSituacaoFundo extends Entity
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
        'situacao' => true,
        'situacao_fundos' => true,
    ];
}
