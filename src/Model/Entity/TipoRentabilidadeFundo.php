<?php
declare(strict_types=1);

namespace App\Model\Entity;

use Cake\ORM\Entity;

/**
 * TipoRentabilidadeFundo Entity
 *
 * @property int $id
 * @property string $rentabilidade
 *
 * @property \App\Model\Entity\CadastroFundo[] $cadastro_fundos
 */
class TipoRentabilidadeFundo extends Entity
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
        'rentabilidade' => true,
        'cadastro_fundos' => true,
    ];
}
